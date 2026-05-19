"""
SRT 推流输出 Handler

将 Avatar 的视频和音频输出通过 SRT 协议推送到 SRS 服务器。
依赖: ffmpeg (需支持 libsrt)

核心设计：同步输出节奏器 (Sync Pacer)
- 视频和音频由同一个线程按固定帧率写入 ffmpeg
- 每个 tick 写入 1 帧视频 + 对应的音频采样 (sample_rate / fps)
- 避免 ffmpeg 两个 raw 输入的独立 demuxer 线程导致 PTS 漂移
- 音频缓冲区不足时自动填充静音，保持时间线连续
"""

import os
import platform
import socket
import subprocess
import threading
import time
import numpy as np
import cv2
from dataclasses import dataclass, field
from typing import Dict, Optional, Set, cast
from collections import deque

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
    ffmpeg_video_port: int = Field(default=9182, description="ffmpeg视频输入端口")
    ffmpeg_audio_port: int = Field(default=9183, description="ffmpeg音频输入端口")
    srt_url_env: str = Field(default="SRT_URL", description="SRT URL 环境变量名")
    video_queue_size: int = Field(default=5, description="视频帧队列大小(帧数)")
    stream_start_timeout_ms: int = Field(default=5000, description="等待音视频都到达的超时时间(毫秒)")
    video_missing_color: str = Field(default="#000000", description="视频缺失时的背景颜色(hex)")


@dataclass
class SRTSession:
    """SRT 会话状态"""
    ffmpeg_process: Optional[subprocess.Popen] = None
    video_writer: Optional[object] = None
    audio_writer: Optional[object] = None
    frame_count: int = 0
    audio_sample_count: int = 0
    # 用单独的锁保护共享状态，I/O 操作在锁外执行
    state_lock: threading.Lock = field(default_factory=threading.Lock)
    # 音频写入锁：防止并发 TCP/FIFO 写入导致数据交错
    audio_write_lock: threading.Lock = field(default_factory=threading.Lock)
    # 视频写入锁
    video_write_lock: threading.Lock = field(default_factory=threading.Lock)
    active_streams: Set[str] = field(default_factory=set)
    _video_server_socket: Optional[socket.socket] = None
    _audio_server_socket: Optional[socket.socket] = None
    _video_ready: threading.Event = field(default_factory=threading.Event)
    _audio_ready: threading.Event = field(default_factory=threading.Event)
    # 启动同步状态
    _first_video_received: bool = False
    _first_audio_received: bool = False
    _stream_start_time: float = 0.0

    def reset(self):
        """重置会话"""
        with self.state_lock:
            self._video_ready.clear()
            self._audio_ready.clear()
            self._pre_connect_audio_buffer.clear()

            # 关闭视频连接
            if self.video_writer is not None:
                try:
                    if isinstance(self.video_writer, socket.socket):
                        self.video_writer.close()
                except Exception:
                    pass
                self.video_writer = None

            if self._video_server_socket is not None:
                try:
                    self._video_server_socket.close()
                except Exception:
                    pass
                self._video_server_socket = None

            # 关闭音频连接
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
                    self.ffmpeg_process.wait(timeout=5)
                except Exception:
                    try:
                        self.ffmpeg_process.kill()
                    except Exception:
                        pass
                self.ffmpeg_process = None

            self.frame_count = 0
            self.audio_sample_count = 0
            self.active_streams.clear()

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

    def _build_ffmpeg_command(self, config: SRTOutputConfig, video_input: str, audio_input: str) -> list:
        """构建 ffmpeg 命令，使用双 TCP 输入"""
        srt_url = self._build_srt_url(config)
        return [
            config.ffmpeg_path,
            "-y", "-hide_banner", "-loglevel", "warning",
            "-f", "rawvideo", "-pix_fmt", "rgb24",
            "-s", f"{config.video_width}x{config.video_height}",
            "-r", str(config.fps),
            "-thread_queue_size", "512",
            "-i", video_input,
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


    def _start_ffmpeg(self, config: SRTOutputConfig) -> SRTSession:
        """使用双 TCP 连接（视频 + 音频）启动 ffmpeg"""
        session = SRTSession()
        session.video_writer = None
        session.audio_writer = None

        # 创建视频 TCP 服务器
        video_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        video_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        video_sock.bind(('127.0.0.1', 0))
        _, video_port = video_sock.getsockname()
        video_sock.listen(1)
        session._video_server_socket = video_sock

        # 创建音频 TCP 服务器
        audio_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        audio_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        audio_sock.bind(('127.0.0.1', 0))
        _, audio_port = audio_sock.getsockname()
        audio_sock.listen(1)
        session._audio_server_socket = audio_sock

        # 等待两个连接，带超时处理
        connection_timeout = 15.0  # 秒

        def accept_video_connection():
            try:
                video_sock.settimeout(connection_timeout)
                conn, _ = video_sock.accept()
                with session.state_lock:
                    session.video_writer = conn
                logger.info(f"SRT: 视频 TCP 连接已建立 (port={video_port})")
                session._video_ready.set()
            except Exception as e:
                logger.error(f"SRT: 等待视频 TCP 连接失败: {e}")
                session._video_ready.set()

        def accept_audio_connection():
            try:
                audio_sock.settimeout(connection_timeout)
                conn, _ = audio_sock.accept()
                with session.state_lock:
                    session.audio_writer = conn
                logger.info(f"SRT: 音频 TCP 连接已建立 (port={audio_port})")
                session._audio_ready.set()
            except Exception as e:
                logger.error(f"SRT: 等待音频 TCP 连接失败: {e}")
                session._audio_ready.set()

        # 设置事件
        session._video_ready = threading.Event()
        session._audio_ready = threading.Event()
        session._video_ready.clear()
        session._audio_ready.clear()

        threading.Thread(target=accept_video_connection, daemon=True, name="srt-video-accept").start()
        threading.Thread(target=accept_audio_connection, daemon=True, name="srt-audio-accept").start()

        video_url = f"tcp://127.0.0.1:{video_port}"
        audio_url = f"tcp://127.0.0.1:{audio_port}"
        command = self._build_ffmpeg_command(config, video_url, audio_url)
        logger.debug(f"Starting ffmpeg (Dual TCP): {' '.join(command)}")

        process = subprocess.Popen(
            command, stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0
        )
        self._start_stderr_reader(process)
        session.ffmpeg_process = process    
        
        # 等待连接建立后再返回
        logger.info("SRT: 等待视频和音频 TCP 连接...")
        return session

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
                HandlerDataInfo(type=ChatDataType.AVATAR_VIDEO, input_priority=-1),
                HandlerDataInfo(type=ChatDataType.AVATAR_AUDIO, input_priority=-1),
            ],
            outputs=[],
            signal_filters=[SignalFilterRule(ChatSignalType.STREAM_CANCEL, None, None)]
        )

    def _ensure_session(self, context: SRTOutputContext) -> Optional[SRTSession]:
        logger.info(f"[SYNC] ===== _ensure_session START ===== session={context.session}, fail_count={context._fail_count}")
        if context.session is not None and context.session.is_running:
            logger.info(f"[SYNC] _ensure_session: 已有运行中的 session, frame_count={context.session.frame_count}")

            # 检查 ffmpeg 进程是否还在运行
            if context.session.ffmpeg_process is not None:
                retcode = context.session.ffmpeg_process.poll()
                if retcode is not None:
                    self._record_failure(context)
                    logger.error(f"[SYNC] _ensure_session: ffmpeg 进程已崩溃, retcode={retcode}")
                    logger.info(f"[SYNC] _ensure_session: ffmpeg崩溃, 重置session")

            # 如果已经有帧输出，说明正常运行
            elif context.session.frame_count > 0:
                logger.info(f"[SYNC] _ensure_session: 运行正常, frame_count={context.session.frame_count}")
                if context._fail_count > 0 and context.session.frame_count > 10:
                    logger.info("SRT: ffmpeg 运行正常，重置重试计数")
                    context._fail_count = 0
                return context.session

            # 检查 TCP 连接是否有效
            elif context.session.video_writer is None or context.session.audio_writer is None:
                logger.warning("[SYNC] _ensure_session: TCP 连接未建立")
                context.session.reset()
                context.session = None

            else:
                logger.info("[SYNC] _ensure_session: 已有 session")
                return context.session

        # 重试冷却检查
        if context._fail_count > 0:
            cooldown = min(5.0 * (2 ** min(context._fail_count - 1, 4)), 60.0)
            elapsed = time.time() - context._last_fail_time
            if elapsed < cooldown:
                logger.warning(f"[SYNC] _ensure_session: 冷却中, 等待{cooldown-elapsed:.1f}s")
                return None
            logger.info(f"[SYNC] _ensure_session: 冷却结束, 已等待{elapsed:.1f}s")

        # 重置旧session
        if context.session is not None:
            logger.info("[SYNC] _ensure_session: 重置旧session")
            context.session.reset()
            context.session = None

        logger.info(f"[SYNC] _ensure_session: 准备启动新ffmpeg, fail_count={context._fail_count}")

        try:
            logger.info("[SYNC] _ensure_session: 调用 _start_ffmpeg...")
            session = self._start_ffmpeg(context.config)
            logger.info(f"[SYNC] _ensure_session: _start_ffmpeg 返回 session={session}")
        except Exception as e:
            logger.exception(f"[SYNC] _ensure_session: _start_ffmpeg 异常: {e}")
            self._record_failure(context)
            return None

        if session is None:
            logger.error("[SYNC] _ensure_session: _start_ffmpeg 返回 None!")
            return None

        context.session = session
        logger.info(f"[SYNC] _ensure_session: ffmpeg启动成功, pid={session.ffmpeg_process.pid if session.ffmpeg_process else 'None'}")

        # 等待两个 TCP 连接都建立，带超时
        logger.info("[SYNC] _ensure_session: 等待视频和音频 TCP 连接...")
        wait_timeout = 10.0  # 最多等待 10 秒
        start_wait = time.time()
        while time.time() - start_wait < wait_timeout:
            video_ready = session._video_ready.is_set() or session.video_writer is not None
            audio_ready = session._audio_ready.is_set() or session.audio_writer is not None

            if video_ready and audio_ready:
                logger.info("[SYNC] _ensure_session: 视频和音频 TCP 都已连接")
                break

            # 检查 ffmpeg 是否崩溃
            if session.ffmpeg_process.poll() is not None:
                logger.error("[SYNC] _ensure_session: ffmpeg 在等待连接时退出")
                return None

            time.sleep(0.1)

        if session.video_writer is None:
            logger.warning("[SYNC] _ensure_session: 视频 TCP 连接超时")
        if session.audio_writer is None:
            logger.warning("[SYNC] _ensure_session: 音频 TCP 连接超时")

        logger.info(f"[SYNC] ===== _ensure_session END (SUCCESS) =====")
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
            video_frame = cv2.resize(video_frame, (target_width, target_height))
            if not HandlerSRTOutput._video_frame_logged:
                logger.info(f"SRT: Resized to {video_frame.shape}")

        return video_frame.tobytes()

    def _check_stream_start(self, session: SRTSession) -> bool:
        """检查是否需要等待两个流都到达

        Returns:
            True: 可以开始发送
            False: 需要等待
        """
        with session.state_lock:
            # 首次调用时记录开始时间
            if session._stream_start_time == 0.0:
                session._stream_start_time = time.time()

            timeout_ms = self.config.stream_start_timeout_ms if self.config else 5000
            elapsed = (time.time() - session._stream_start_time) * 1000

            if session._first_video_received and session._first_audio_received:
                return True

            if elapsed > timeout_ms:
                # 超时，一个到了另一个没到
                if not session._first_video_received:
                    logger.warning(f"SRT: 视频流超时({elapsed:.0f}ms), 将使用背景色")
                    session._first_video_received = True
                if not session._first_audio_received:
                    logger.warning(f"SRT: 音频流超时({elapsed:.0f}ms), 将使用静音")
                    session._first_audio_received = True
                return True

            # 计算剩余等待时间
            remaining = timeout_ms - elapsed
            if remaining > 0:
                logger.info(f"SRT: 等待音视频到达... video={session._first_video_received}, "
                           f"audio={session._first_audio_received}, 剩余{remaining:.0f}ms")
                return False

        return True

    def _write_video(self, session: SRTSession, video_frame: np.ndarray):
        """直接发送视频帧到 TCP 连接"""
        if not session.is_running:
            return False

        if video_frame is None or video_frame.size == 0:
            return True

        try:
            # 检查启动同步状态
            if not session._first_video_received:
                session._first_video_received = True
                if not self._check_stream_start(session):
                    logger.info("SRT: 等待音频流到达，暂不发送视频")
                    return True

            rgb_bytes = self._prepare_video_frame(video_frame)

            # 直接发送到 TCP
            with session.state_lock:
                writer = session.video_writer

            if writer is None:
                logger.warning("SRT: 视频 TCP 未连接，跳过帧")
                return True

            with session.video_write_lock:
                writer.sendall(rgb_bytes)

            session.frame_count += 1
            return True

        except Exception as e:
            logger.error(f"SRT: 发送视频帧失败: {e}")
            return False

    def _write_audio(self, session: SRTSession, audio_data: np.ndarray):
        """直接发送音频数据到 TCP 连接"""
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

            # 检查启动同步状态
            if not session._first_audio_received:
                session._first_audio_received = True
                if not self._check_stream_start(session):
                    logger.info("SRT: 等待视频流到达，暂不发送音频")
                    return True

            audio_bytes = audio_data.tobytes()

            # 直接发送到 TCP
            with session.state_lock:
                writer = session.audio_writer

            if writer is None:
                logger.warning("SRT: 音频 TCP 未连接，跳过")
                return True

            with session.audio_write_lock:
                writer.sendall(audio_bytes)

            session.audio_sample_count += len(audio_data)
            return True

        except Exception as e:
            logger.error(f"SRT: 发送音频失败: {e}")
            return False

    def handle(self, context: HandlerContext, inputs: ChatData,
               output_definitions: Dict[ChatDataType, HandlerDataInfo]):
        logger.info(f"[SYNC] SRT handle() called: type={inputs.type}, stream_id={inputs.stream_id}")
        context = cast(SRTOutputContext, context)

        input_stream = inputs.stream_id
        stream_key_str = str(input_stream.key) if input_stream and input_stream.key else "unknown"

        logger.info("[SYNC] SRT: 调用 _ensure_session 获取 session...")
        session = self._ensure_session(context)
        if session is None:
            logger.error(f"[SYNC] SRT handle: _ensure_session 返回 None! fail_count={context._fail_count}, type={inputs.type}")
            return
        
        logger.info(f"[SYNC] SRT handle: session 成功获取, frame_count={session.frame_count}")

        stream_id_str = f"{inputs.type.value}:{stream_key_str}"
        with session.state_lock:
            if stream_id_str not in session.active_streams:
                session.active_streams.add(stream_id_str)
                logger.info(f"[SYNC] SRT: 新流加入 {stream_id_str}")

        logger.info(f"[SYNC] SRT: 准备写入数据, type={inputs.type}")
        if inputs.type == ChatDataType.AVATAR_VIDEO:
            video_frame = inputs.data.get_main_data()
            logger.info(f"[SYNC] SRT: 写入视频帧, shape={video_frame.shape}")
            if not self._write_video(session, video_frame):
                logger.error(f"[SYNC] SRT: _write_video 失败!")
                self._record_failure(context)
                session.reset()
                context.session = None

        elif inputs.type == ChatDataType.AVATAR_AUDIO:
            audio_data = inputs.data.get_main_data()
            logger.info(f"[SYNC] SRT: 写入音频, shape={audio_data.shape}")
            if not self._write_audio(session, audio_data):
                logger.error(f"[SYNC] SRT: _write_audio 失败!")
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
        
        logger.info(f"[SYNC] SRT handle 完成, type={inputs.type}")

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
