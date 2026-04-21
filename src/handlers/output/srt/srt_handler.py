"""
SRT 推流输出 Handler

将 Avatar 的视频和音频输出通过 SRT 协议推送到 SRS 服务器
依赖: ffmpeg (需支持 libsrt)

视频通过队列 + 后台线程写入 stdin，避免 I/O 阻塞 pumper 线程。
音频通过命名管道(FIFO)/TCP 传输。
所有 I/O 写操作在锁外执行，避免死锁。
"""

import os
import platform
import queue
import socket
import subprocess
import tempfile
import threading
import time
import numpy as np
from dataclasses import dataclass, field
from typing import Dict, Optional, Set, cast

from loguru import logger
from pydantic import BaseModel, Field

from chat_engine.common.handler_base import HandlerBase, HandlerBaseInfo, HandlerDataInfo, HandlerDetail
from chat_engine.contexts.handler_context import HandlerContext
from chat_engine.contexts.session_context import SessionContext
from chat_engine.data_models.chat_data.chat_data_model import ChatData
from chat_engine.data_models.chat_data_type import ChatDataType
from chat_engine.data_models.chat_engine_config_data import HandlerBaseConfigModel, ChatEngineConfigModel
from chat_engine.data_models.chat_signal import ChatSignal, SignalFilterRule
from chat_engine.data_models.chat_signal_type import ChatSignalType
from chat_engine.data_models.chat_stream import StreamKey, ChatStreamIdentity
from chat_engine.data_models.chat_stream_config import ChatStreamConfig
from chat_engine.data_models.runtime_data.data_bundle import DataBundle, DataBundleDefinition, DataBundleEntry

IS_WINDOWS = platform.system() == 'Windows'


class SRTOutputConfig(HandlerBaseConfigModel, BaseModel):
    """SRT 输出配置"""
    srt_url: str = Field(
        default="srt://127.0.0.1:10080?streamid=#!::r=live/avatar,m=publish",
        description="SRT 推流地址"
    )
    latency_ms: int = Field(default=120, description="SRT 延迟(ms)")
    video_width: int = Field(default=512, description="视频宽度")
    video_height: int = Field(default=512, description="视频高度")
    fps: int = Field(default=25, description="视频帧率")
    video_bitrate: int = Field(default=2000, description="视频码率(kbps)")
    audio_sample_rate: int = Field(default=24000, description="音频输入采样率")
    audio_bitrate: int = Field(default=128, description="音频码率(kbps)")
    preset: str = Field(default="fast", description="x264 预设")
    tune: str = Field(default="zerolatency", description="x264 tune 参数")
    ffmpeg_path: str = Field(default="ffmpeg", description="ffmpeg 可执行文件路径")
    srt_url_env: str = Field(default="SRT_URL", description="SRT URL 环境变量名")
    video_queue_size: int = Field(default=30, description="视频帧队列大小(帧数)")


@dataclass
class SRTSession:
    """SRT 会话状态"""
    ffmpeg_process: Optional[subprocess.Popen] = None
    audio_writer: Optional[object] = None
    audio_fifo_path: Optional[str] = None
    tmp_dir: Optional[str] = None
    frame_count: int = 0
    audio_sample_count: int = 0
    # 用单独的锁保护共享状态，I/O 操作在锁外执行
    state_lock: threading.Lock = field(default_factory=threading.Lock)
    # 音频写入锁：防止并发 TCP/FIFO 写入导致数据交错
    audio_write_lock: threading.Lock = field(default_factory=threading.Lock)
    active_streams: Set[str] = field(default_factory=set)
    _audio_server_socket: Optional[socket.socket] = None
    _audio_ready: threading.Event = field(default_factory=threading.Event)
    _audio_buffer: list = field(default_factory=list)
    # 视频帧队列 + 后台写线程
    _video_queue: queue.Queue = field(default_factory=lambda: queue.Queue(maxsize=30))
    _video_writer_thread: Optional[threading.Thread] = None
    _video_writer_quit: threading.Event = field(default_factory=threading.Event)
    _video_drop_count: int = 0
    # 最后一帧的 RGB 数据（用于丢帧时补写，保持时间线连续）
    _last_video_bytes: Optional[bytes] = None

    def reset(self):
        """重置会话"""
        # 先停止视频写线程
        self._video_writer_quit.set()
        try:
            self._video_queue.put(None, timeout=0.5)  # sentinel
        except queue.Full:
            pass
        if self._video_writer_thread is not None:
            self._video_writer_thread.join(timeout=3)
            self._video_writer_thread = None

        with self.state_lock:
            self._audio_ready.clear()
            self._audio_buffer.clear()

            if self.audio_writer is not None:
                try:
                    if isinstance(self.audio_writer, socket.socket):
                        self.audio_writer.close()
                    elif isinstance(self.audio_writer, int):
                        os.close(self.audio_writer)
                except Exception:
                    pass
                self.audio_writer = None

            if self._audio_server_socket is not None:
                try:
                    self._audio_server_socket.close()
                except Exception:
                    pass
                self._audio_server_socket = None

            if self.ffmpeg_process is not None:
                try:
                    if self.ffmpeg_process.stdin:
                        self.ffmpeg_process.stdin.close()
                except Exception:
                    pass
                try:
                    self.ffmpeg_process.wait(timeout=5)
                except Exception:
                    try:
                        self.ffmpeg_process.kill()
                    except Exception:
                        pass
                self.ffmpeg_process = None

            if self.audio_fifo_path and os.path.exists(self.audio_fifo_path):
                try:
                    os.unlink(self.audio_fifo_path)
                except Exception:
                    pass
                self.audio_fifo_path = None

            if self.tmp_dir and os.path.exists(self.tmp_dir):
                try:
                    os.rmdir(self.tmp_dir)
                except Exception:
                    pass
                self.tmp_dir = None

            self.frame_count = 0
            self.audio_sample_count = 0
            self.active_streams.clear()
            self._video_drop_count = 0
            self._last_video_bytes = None

        # 清空队列
        while not self._video_queue.empty():
            try:
                self._video_queue.get_nowait()
            except queue.Empty:
                break
        self._video_writer_quit.clear()

    @property
    def is_running(self) -> bool:
        return self.ffmpeg_process is not None and self.ffmpeg_process.poll() is None


class SRTOutputContext(HandlerContext):
    def __init__(self, session_id: str):
        super().__init__(session_id)
        self.config: Optional[SRTOutputConfig] = None
        self.session: Optional[SRTSession] = None
        # 重试控制
        self._fail_count: int = 0
        self._last_fail_time: float = 0.0


class HandlerSRTOutput(HandlerBase):
    def __init__(self):
        super().__init__()
        self.config: Optional[SRTOutputConfig] = None

    def get_handler_info(self) -> HandlerBaseInfo:
        return HandlerBaseInfo(config_model=SRTOutputConfig)

    def load(self, engine_config: ChatEngineConfigModel, handler_config: Optional[BaseModel] = None):
        self.config = cast(SRTOutputConfig, handler_config or SRTOutputConfig())
        if self.config.srt_url_env in os.environ:
            self.config.srt_url = os.getenv(self.config.srt_url_env, self.config.srt_url)
        try:
            result = subprocess.run(
                [self.config.ffmpeg_path, "-protocols"],
                capture_output=True, text=True, timeout=5
            )
            if "srt" not in result.stdout:
                logger.warning(f"ffmpeg 可能不支持 SRT 协议: {self.config.ffmpeg_path}")
            else:
                logger.info(f"ffmpeg SRT 支持已确认")
        except Exception as e:
            logger.error(f"检查 ffmpeg 失败: {e}")
        logger.info(f"SRT Output Handler loaded, url={self.config.srt_url}, "
                   f"video={self.config.video_width}x{self.config.video_height}@{self.config.fps}fps")

    def _build_srt_url(self, config: SRTOutputConfig) -> str:
        srt_url = config.srt_url
        if "latency=" not in srt_url:
            srt_url = f"{srt_url}&latency={config.latency_ms * 1000}"
        return srt_url

    def _build_ffmpeg_command(self, config: SRTOutputConfig, audio_input: str) -> list:
        srt_url = self._build_srt_url(config)
        return [
            config.ffmpeg_path,
            "-y", "-hide_banner", "-loglevel", "warning",
            "-f", "rawvideo", "-pix_fmt", "rgb24",
            "-s", f"{config.video_width}x{config.video_height}",
            "-r", str(config.fps),
            "-thread_queue_size", "512",
            "-i", "-",
            "-f", "f32le", "-ar", str(config.audio_sample_rate), "-ac", "1",
            "-thread_queue_size", "512",
            "-i", audio_input,
            "-c:v", "libx264",
            "-preset", config.preset,
            "-tune", config.tune,
            "-b:v", f"{config.video_bitrate}k",
            "-pix_fmt", "yuv420p",
            "-g", str(config.fps * 2),
            "-keyint_min", str(config.fps),
            "-sc_threshold", "0",
            "-af", "aresample=async=1:first_pts=0",
            "-c:a", "aac",
            "-b:a", f"{config.audio_bitrate}k",
            "-ar", "44100",
            "-ac", "2",
            "-f", "mpegts",
            "-flush_packets", "1",
            srt_url
        ]

    def _start_stderr_reader(self, process: subprocess.Popen):
        def read_stderr():
            while True:
                try:
                    line = process.stderr.readline()
                    if not line:
                        if process.poll() is not None:
                            remaining = process.stderr.read()
                            if remaining:
                                for l in remaining.decode('utf-8', errors='ignore').strip().split('\n'):
                                    if l:
                                        logger.warning(f"ffmpeg: {l}")
                            break
                        continue
                    text = line.decode('utf-8', errors='ignore').strip()
                    if text:
                        logger.debug(f"ffmpeg: {text}")
                except Exception:
                    break
        threading.Thread(target=read_stderr, daemon=True).start()

    def _start_video_writer(self, session: SRTSession):
        """启动后台视频写线程，从队列取帧写入 ffmpeg stdin"""
        session._video_writer_quit.clear()

        def writer_loop():
            while not session._video_writer_quit.is_set():
                try:
                    item = session._video_queue.get(timeout=0.1)
                    if item is None:  # sentinel
                        break
                    rgb_bytes, count = item

                    # 检查 ffmpeg 进程状态
                    if session.ffmpeg_process is None or session.ffmpeg_process.poll() is not None:
                        retcode = session.ffmpeg_process.poll() if session.ffmpeg_process else None
                        logger.error(f"SRT: ffmpeg 已退出 (code={retcode}), 停止视频写入")
                        break

                    try:
                        stdin = session.ffmpeg_process.stdin
                        if stdin is None:
                            logger.error("SRT: ffmpeg stdin is None")
                            break
                        stdin.write(rgb_bytes)
                        if count == 1:
                            logger.info("[SYNC] SRT: First video frame written to ffmpeg stdin")
                        if count % 500 == 0:
                            logger.info(f"SRT: Sent {count} video frames")
                    except BrokenPipeError:
                        logger.error("SRT: ffmpeg stdin pipe broken (video writer thread)")
                        break
                    except OSError as e:
                        logger.error(f"SRT: ffmpeg stdin write error: {e}")
                        break
                except queue.Empty:
                    continue

            # 清空队列
            while not session._video_queue.empty():
                try:
                    session._video_queue.get_nowait()
                except queue.Empty:
                    break

            logger.debug("SRT: Video writer thread stopped")

        thread = threading.Thread(target=writer_loop, daemon=True, name="srt-video-writer")
        thread.start()
        session._video_writer_thread = thread

    def _start_ffmpeg_with_fifo(self, config: SRTOutputConfig) -> SRTSession:
        tmp_dir = tempfile.mkdtemp(prefix='srt_')
        audio_fifo = os.path.join(tmp_dir, 'audio')
        os.mkfifo(audio_fifo)

        command = self._build_ffmpeg_command(config, audio_fifo)
        logger.debug(f"Starting ffmpeg (FIFO): {' '.join(command)}")

        process = subprocess.Popen(
            command, stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0
        )
        self._start_stderr_reader(process)

        session = SRTSession()
        session.ffmpeg_process = process
        session.audio_fifo_path = audio_fifo
        session.tmp_dir = tmp_dir

        # 启动视频写线程
        self._start_video_writer(session)

        def open_audio_fifo():
            try:
                fd = os.open(audio_fifo, os.O_WRONLY)
                # 在锁内更新状态，丢弃旧缓冲（不刷给 ffmpeg，避免时间线偏移）
                with session.state_lock:
                    session.audio_writer = fd
                    dropped_chunks = len(session._audio_buffer)
                    dropped_bytes = sum(len(b) for b in session._audio_buffer)
                    session._audio_buffer.clear()
                if dropped_chunks > 0:
                    logger.info(f"[SYNC] SRT: 音频 FIFO 已打开，丢弃 {dropped_chunks} 块旧缓冲 "
                               f"({dropped_bytes} bytes)，音视频从当前时间同步开始")
                else:
                    logger.info("SRT: 音频 FIFO 已打开")
                session._audio_ready.set()
            except Exception as e:
                logger.error(f"SRT: 打开音频 FIFO 失败: {e}")
                session._audio_ready.set()

        threading.Thread(target=open_audio_fifo, daemon=True).start()
        return session

    def _start_ffmpeg_with_tcp(self, config: SRTOutputConfig) -> SRTSession:
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind(('127.0.0.1', 0))
        _, audio_port = server_sock.getsockname()
        server_sock.listen(1)

        audio_url = f"tcp://127.0.0.1:{audio_port}"
        command = self._build_ffmpeg_command(config, audio_url)
        logger.debug(f"Starting ffmpeg (TCP): {' '.join(command)}")

        process = subprocess.Popen(
            command, stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0
        )
        self._start_stderr_reader(process)

        session = SRTSession()
        session.ffmpeg_process = process
        session._audio_server_socket = server_sock

        # 启动视频写线程
        self._start_video_writer(session)

        def accept_audio_connection():
            try:
                server_sock.settimeout(15)
                conn, _ = server_sock.accept()
                # 在锁内更新状态，丢弃旧缓冲（不刷给 ffmpeg，避免时间线偏移）
                with session.state_lock:
                    session.audio_writer = conn
                    dropped_chunks = len(session._audio_buffer)
                    dropped_bytes = sum(len(b) for b in session._audio_buffer)
                    session._audio_buffer.clear()
                if dropped_chunks > 0:
                    logger.info(f"[SYNC] SRT: 音频 TCP 连接已建立，丢弃 {dropped_chunks} 块旧缓冲 "
                               f"({dropped_bytes} bytes)，音视频从当前时间同步开始")
                else:
                    logger.info("SRT: 音频 TCP 连接已建立")
                session._audio_ready.set()
            except Exception as e:
                logger.error(f"SRT: 等待音频 TCP 连接失败: {e}")
                session._audio_ready.set()

        threading.Thread(target=accept_audio_connection, daemon=True).start()
        return session

    def _start_ffmpeg(self, config: SRTOutputConfig) -> SRTSession:
        if IS_WINDOWS:
            return self._start_ffmpeg_with_tcp(config)
        else:
            return self._start_ffmpeg_with_fifo(config)

    def create_context(self, session_context: SessionContext,
                       handler_config: Optional[BaseModel] = None) -> HandlerContext:
        context = SRTOutputContext(session_context.session_info.session_id)
        context.config = cast(SRTOutputConfig, handler_config or self.config)
        return context

    def start_context(self, session_context: SessionContext, handler_context: HandlerContext):
        pass

    def get_handler_detail(self, session_context: SessionContext,
                           context: HandlerContext) -> HandlerDetail:
        return HandlerDetail(
            inputs=[
                HandlerDataInfo(type=ChatDataType.AVATAR_VIDEO),
                HandlerDataInfo(type=ChatDataType.AVATAR_AUDIO),
            ],
            outputs=[],
            signal_filters=[SignalFilterRule(ChatSignalType.STREAM_CANCEL, None, None)]
        )

    def _ensure_session(self, context: SRTOutputContext) -> Optional[SRTSession]:
        if context.session is not None and context.session.is_running:
            # 检查视频写线程是否存活
            if context.session._video_writer_thread is not None and not context.session._video_writer_thread.is_alive():
                logger.error("SRT: Video writer thread died, resetting session")
                self._record_failure(context)
                context.session.reset()
                context.session = None
            else:
                # 运行正常，重置失败计数
                if context._fail_count > 0 and context.session.frame_count > 10:
                    logger.info("SRT: ffmpeg 运行正常，重置重试计数")
                    context._fail_count = 0
                return context.session

        # 重试冷却检查（指数退避：5, 10, 20, 40, 60s）
        if context._fail_count > 0:
            cooldown = min(5.0 * (2 ** min(context._fail_count - 1, 4)), 60.0)
            elapsed = time.time() - context._last_fail_time
            if elapsed < cooldown:
                return None  # 冷却中，不重试

        if context.session is not None:
            context.session.reset()
            context.session = None

        if context._fail_count > 0:
            logger.info(f"SRT: 重试启动 ffmpeg (第 {context._fail_count + 1} 次)")

        try:
            session = self._start_ffmpeg(context.config)
        except Exception as e:
            logger.error(f"SRT Output: 启动 ffmpeg 失败: {e}")
            self._record_failure(context)
            return None

        context.session = session
        return session

    def _record_failure(self, context: SRTOutputContext):
        """记录失败并输出诊断信息"""
        context._fail_count += 1
        context._last_fail_time = time.time()
        if context._fail_count == 1:
            logger.error(
                f"SRT: ffmpeg 连接失败！请确认 SRS 服务器是否在 "
                f"{context.config.srt_url} 上运行。"
                f"后续将自动重试（指数退避）。"
            )

    _video_frame_logged = False  # 只记录第一帧的详细信息

    def _prepare_video_frame(self, video_frame: np.ndarray) -> Optional[bytes]:
        """预处理视频帧：squeeze、BGR->RGB、resize，返回 RGB bytes"""
        # 去除批次维度 (1, H, W, 3) -> (H, W, 3)
        if video_frame.ndim == 4 and video_frame.shape[0] == 1:
            video_frame = video_frame.squeeze(axis=0)

        if not HandlerSRTOutput._video_frame_logged:
            logger.info(f"SRT: Video frame original shape={video_frame.shape}, dtype={video_frame.dtype}")
            HandlerSRTOutput._video_frame_logged = True

        # 确保格式正确 (uint8)
        if video_frame.dtype != np.uint8:
            video_frame = (video_frame * 255).clip(0, 255).astype(np.uint8)

        # 确保是 3 通道
        if video_frame.ndim == 2:
            video_frame = np.stack([video_frame] * 3, axis=-1)
        elif video_frame.shape[2] == 4:
            video_frame = video_frame[:, :, :3]

        # BGR -> RGB
        video_frame = video_frame[:, :, ::-1]

        # 调整尺寸
        target_width = self.config.video_width if self.config.video_width > 0 else 512
        target_height = self.config.video_height if self.config.video_height > 0 else 512

        if video_frame.shape[0] != target_height or video_frame.shape[1] != target_width:
            import cv2
            video_frame = cv2.resize(video_frame, (target_width, target_height))
            if not HandlerSRTOutput._video_frame_logged:
                logger.info(f"SRT: Resized to {video_frame.shape}")

        return video_frame.tobytes()

    def _write_video(self, session: SRTSession, video_frame: np.ndarray):
        """将视频帧放入队列，由后台线程写入 ffmpeg stdin（非阻塞）"""
        if not session.is_running:
            return False

        if video_frame is None or video_frame.size == 0:
            return True

        # 等待音频连接就绪再写入视频帧，确保音视频同步启动
        if session.frame_count == 0:
            if not session._audio_ready.wait(timeout=3.0):
                logger.warning("[SYNC] SRT: 音频连接未就绪(3s超时)，首帧视频先写入")

        try:
            rgb_bytes = self._prepare_video_frame(video_frame)

            # 在锁内获取引用和更新计数
            with session.state_lock:
                if session.ffmpeg_process is None or session.ffmpeg_process.poll() is not None:
                    logger.warning("SRT: ffmpeg process not running, cannot queue video frame")
                    return False
                session.frame_count += 1
                count = session.frame_count

            if count <= 3:
                logger.info(f"SRT: Queuing video frame #{count}, {len(rgb_bytes)} bytes")

            # 非阻塞放入队列；队列满时补写上一帧副本以保持时间线连续
            try:
                session._video_queue.put_nowait((rgb_bytes, count))
                session._last_video_bytes = rgb_bytes
            except queue.Full:
                # 队列满，用上一帧补写（保持 ffmpeg 时间线连续，避免丢帧压缩时间线导致音视频不同步）
                last_bytes = session._last_video_bytes
                if last_bytes is not None:
                    try:
                        session._video_queue.put_nowait((last_bytes, -count))
                    except queue.Full:
                        pass
                session._video_drop_count += 1
                if session._video_drop_count <= 3 or session._video_drop_count % 100 == 0:
                    logger.warning(f"[SYNC] SRT: Video queue full, duplicated last frame for #{count} "
                                 f"(total: {session._video_drop_count})")
                return True

            # 检查写线程是否存活
            if session._video_writer_thread is not None and not session._video_writer_thread.is_alive():
                logger.error("SRT: Video writer thread died, ffmpeg likely crashed")
                return False

            return True

        except Exception as e:
            logger.error(f"SRT: Failed to queue video frame: {e}")
            return False

    def _write_audio(self, session: SRTSession, audio_data: np.ndarray):
        """写入音频数据（I/O 在锁外执行）"""
        if not session.is_running:
            return False

        if audio_data is None or audio_data.size == 0:
            return True

        try:
            # 展平 + 转换为归一化 float32
            audio_data = audio_data.flatten()
            if audio_data.dtype == np.int16:
                audio_data = audio_data.astype(np.float32) / 32768.0
            elif audio_data.dtype != np.float32:
                audio_data = audio_data.astype(np.float32)
                max_val = np.abs(audio_data).max()
                if max_val > 1.0:
                    audio_data = audio_data / max_val

            audio_bytes = audio_data.tobytes()

            # 在锁内获取 writer 和缓冲区
            with session.state_lock:
                writer = session.audio_writer
                if writer is not None:
                    session.audio_sample_count += len(audio_data)
                    first_audio = session.audio_sample_count == len(audio_data)
                else:
                    if len(session._audio_buffer) < 100:
                        session._audio_buffer.append(audio_bytes)
                    else:
                        logger.warning("[SYNC] SRT: 音频缓冲已满，丢弃数据")
                    return True

            if first_audio:
                logger.info(f"[SYNC] SRT: First audio written, {len(audio_data)} samples")

            # 锁外执行 I/O（使用 audio_write_lock 防止并发写入导致数据交错）
            with session.audio_write_lock:
                if isinstance(writer, socket.socket):
                    writer.sendall(audio_bytes)
                elif isinstance(writer, int):
                    os.write(writer, audio_bytes)

            return True

        except (BrokenPipeError, OSError, ConnectionError) as e:
            logger.error(f"SRT: 写入音频数据失败: {e}")
            return False
        except Exception as e:
            logger.error(f"SRT: 写入音频数据失败: {e}")
            return False

    def handle(self, context: HandlerContext, inputs: ChatData,
               output_definitions: Dict[ChatDataType, HandlerDataInfo]):
        context = cast(SRTOutputContext, context)

        input_stream = inputs.stream_id
        stream_key_str = str(input_stream.key) if input_stream and input_stream.key else "unknown"

        session = self._ensure_session(context)
        if session is None:
            return

        stream_id_str = f"{inputs.type.value}:{stream_key_str}"
        with session.state_lock:
            if stream_id_str not in session.active_streams:
                session.active_streams.add(stream_id_str)
                logger.info(f"[SYNC] SRT: 新流加入 {stream_id_str}")

        now = time.time()

        if inputs.type == ChatDataType.AVATAR_VIDEO:
            video_frame = inputs.data.get_main_data()
            with session.state_lock:
                vc = session.frame_count
            # 每100帧打印一次音视频计数差，用于检测漂移
            if vc > 0 and vc % 100 == 0:
                audio_dur = session.audio_sample_count / (context.config.audio_sample_rate or 24000)
                video_dur = vc / (context.config.fps or 25)
                # 实际写入的帧数 = 总帧数 - 丢帧数（丢帧用上一帧补写，时间线未压缩）
                actual_written = vc - session._video_drop_count
                actual_video_dur = actual_written / (context.config.fps or 25)
                drift_ms = (actual_video_dur - audio_dur) * 1000
                logger.info(f"[SYNC] frame={vc}(written={actual_written}) audio_samples={session.audio_sample_count} "
                           f"video_dur={actual_video_dur:.2f}s audio_dur={audio_dur:.2f}s drift={drift_ms:+.0f}ms")
            if not self._write_video(session, video_frame):
                self._record_failure(context)
                session.reset()
                context.session = None

        elif inputs.type == ChatDataType.AVATAR_AUDIO:
            audio_data = inputs.data.get_main_data()
            if not self._write_audio(session, audio_data):
                self._record_failure(context)
                session.reset()
                context.session = None

            if inputs.is_last_data:
                with session.state_lock:
                    session.active_streams.discard(stream_id_str)
                    remaining = set(session.active_streams)
                if not remaining:
                    session.reset()
                    context.session = None

    def on_signal(self, context: HandlerContext, signal: ChatSignal):
        context = cast(SRTOutputContext, context)
        if signal.type == ChatSignalType.STREAM_CANCEL:
            logger.info("SRT Output: 收到取消信号")
            if context.session is not None:
                context.session.reset()
                context.session = None

    def destroy_context(self, context: HandlerContext):
        context = cast(SRTOutputContext, context)
        logger.info("SRT Output: 销毁上下文")
        if context.session is not None:
            try:
                context.session.reset()
            except Exception as e:
                logger.warning(f"重置会话失败: {e}")
            context.session = None
