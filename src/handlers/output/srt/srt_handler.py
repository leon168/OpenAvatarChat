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
import queue
import socket
import subprocess
import tempfile
import threading
import time
import numpy as np
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
    video_queue_size: int = Field(default=5, description="视频帧队列大小(帧数)")


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
    # 视频帧队列（由同步节奏器消费）
    _video_queue: "deque" = field(default_factory=deque)
    _video_queue_lock: threading.Lock = field(default_factory=threading.Lock)
    _video_queue_not_empty: threading.Condition = field(default_factory=lambda: threading.Condition())
    # 音频原始数据缓冲（float32 采样，由同步节奏器消费）
    _audio_buffer: bytearray = field(default_factory=bytearray)
    _audio_buffer_lock: threading.Lock = field(default_factory=threading.Lock)
    _audio_buffer_event: threading.Event = field(default_factory=threading.Event)
    # 同步节奏器线程
    _pacer_thread: Optional[threading.Thread] = None
    _pacer_quit: threading.Event = field(default_factory=threading.Event)
    # 最后一帧的 RGB 数据（用于丢帧时补写，保持时间线连续）
    _last_video_bytes: Optional[bytes] = None
    # pacer 启动时间（用于检测卡死）
    _pacer_start_time: float = 0.0
    # 启动前的音频缓冲（TCP/FIFO 未连接时暂存）
    _pre_connect_audio_buffer: list = field(default_factory=list)

    def reset(self):
        """重置会话"""
        # 先停止节奏器线程
        self._pacer_quit.set()
        self._audio_buffer_event.set()
        with self._video_queue_lock:
            self._video_queue.clear()
        with self._video_queue_not_empty:
            self._video_queue_not_empty.notify_all()

        if self._pacer_thread is not None:
            self._pacer_thread.join(timeout=3)
            self._pacer_thread = None

        with self.state_lock:
            self._audio_ready.clear()
            self._pre_connect_audio_buffer.clear()

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
            self._last_video_bytes = None

        with self._audio_buffer_lock:
            self._audio_buffer.clear()
        self._pacer_quit.clear()

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

    def _start_sync_pacer(self, session: SRTSession, config: SRTOutputConfig):
        """启动同步节奏器线程：每个 tick 写 1 帧视频 + 对应音频

        核心设计：视频和音频由同一线程、同一步调写入 ffmpeg，
        确保 ffmpeg 的两个 raw 输入 demuxer 线程同步推进 PTS，
        从根本上消除音视频漂移问题。
        """
        session._pacer_quit.clear()
        fps = config.fps
        sample_rate = config.audio_sample_rate
        samples_per_frame = sample_rate // fps  # 每帧对应的音频采样数 (24000/25=960)
        frame_duration = 1.0 / fps  # 帧间隔 (40ms)
        max_queue_size = config.video_queue_size

        # float32 每采样 4 字节
        audio_bytes_per_frame = samples_per_frame * 4

        def pacer_loop():
            # 等待音频连接就绪
            # 必须等音频就绪后才开始写数据，否则 ffmpeg 视频 PTS 领先音频 PTS，
            # 造成永久性音视频偏移
            audio_wait_start = time.time()
            while not session._pacer_quit.is_set():
                if session._audio_ready.wait(timeout=0.5):
                    logger.info("[SYNC] Pacer: Audio connection ready, starting synced output")
                    break
                elapsed = time.time() - audio_wait_start
                # 每 2 秒报告一次状态
                if int(elapsed * 2) % 2 == 0 and int((elapsed - 0.5) * 2) % 2 != 0:
                    logger.warning(f"[SYNC] Pacer: Still waiting for audio connection ({elapsed:.0f}s)")
                # 检查 ffmpeg 是否还在运行
                if session.ffmpeg_process is None or session.ffmpeg_process.poll() is not None:
                    retcode = session.ffmpeg_process.poll() if session.ffmpeg_process else -1
                    logger.error(f"[SYNC] Pacer: ffmpeg exited before audio ready (code={retcode})")
                    return
                # 30 秒超时：如果音频连接 30 秒还没建立，放弃本次尝试
                if elapsed > 30.0:
                    logger.error("[SYNC] Pacer: Audio connection not ready after 30s, giving up")
                    return
            else:
                logger.error("[SYNC] Pacer: Quit signal while waiting for audio")
                return

            logger.info(f"[SYNC] Pacer started: {fps}fps, {samples_per_frame} samples/frame, "
                       f"{frame_duration*1000:.0f}ms/frame")

            # 等待第一帧视频到达（避免空跑）
            first_frame_wait = 0
            while not session._pacer_quit.is_set():
                with session._video_queue_lock:
                    if session._video_queue:
                        break
                time.sleep(0.01)
                first_frame_wait += 10
                if first_frame_wait > 5000:  # 5s 超时
                    logger.error("[SYNC] Pacer: No video frame received after 5s, stopping")
                    return
                # 检查 ffmpeg 进程
                if session.ffmpeg_process is None or session.ffmpeg_process.poll() is not None:
                    retcode = session.ffmpeg_process.poll() if session.ffmpeg_process else -1
                    logger.error(f"[SYNC] Pacer: ffmpeg exited before first frame (code={retcode})")
                    return

            start_time = None
            tick = 0
            silence_audio = b'\x00' * audio_bytes_per_frame  # 静音帧
            consecutive_silence = 0
            # 视频写入队列：pacer 放入帧数据，视频写线程消费
            # 避免 stdin.write(2.28MB) 阻塞 pacer 节奏控制
            video_write_queue = queue.Queue(maxsize=5)
            video_write_error = [False]  # 用 list 以便在闭包中修改

            def video_writer():
                """独立线程写视频帧到 ffmpeg stdin，避免阻塞 pacer"""
                while not session._pacer_quit.is_set():
                    try:
                        item = video_write_queue.get(timeout=0.1)
                        if item is None:
                            break
                        v_bytes, v_tick = item
                        if video_write_error[0]:
                            continue
                        try:
                            stdin = session.ffmpeg_process.stdin
                            if stdin is not None:
                                stdin.write(v_bytes)
                        except BrokenPipeError:
                            logger.error("[SYNC] Video writer: stdin pipe broken")
                            video_write_error[0] = True
                        except OSError as e:
                            logger.error(f"[SYNC] Video writer: stdin error: {e}")
                            video_write_error[0] = True
                    except queue.Empty:
                        continue

            vw_thread = threading.Thread(target=video_writer, daemon=True, name="srt-video-writer")
            vw_thread.start()

            # 裁剪音频缓冲区：确保音视频对齐
            # pacer 启动前音频可能堆积了几秒（等待连接期间），
            # 但视频队列有上限会丢旧帧，所以必须裁掉音频多余部分
            with session._video_queue_lock:
                vq_len = len(session._video_queue)
            with session._audio_buffer_lock:
                max_audio_bytes = (vq_len + 1) * audio_bytes_per_frame  # 比视频多1帧余量
                if len(session._audio_buffer) > max_audio_bytes:
                    excess = len(session._audio_buffer) - max_audio_bytes
                    del session._audio_buffer[:excess]
                    logger.info(f"[SYNC] Pacer: Trimmed audio buffer by {excess} bytes "
                               f"({excess / 4 / sample_rate:.2f}s), "
                               f"remaining={len(session._audio_buffer) / 4 / sample_rate:.2f}s, "
                               f"video_q={vq_len} frames")

            while not session._pacer_quit.is_set():
                # 1. 计算目标写入时间
                if start_time is None:
                    start_time = time.perf_counter()

                target_time = start_time + tick * frame_duration

                # 2. 等待到目标时间
                now = time.perf_counter()
                wait = target_time - now
                if wait > 0.001:  # > 1ms 才 sleep
                    time.sleep(wait * 0.9)  # 留 10% 余量给 spin-wait
                    while time.perf_counter() < target_time:
                        pass  # spin-wait 精确对齐
                elif wait < -frame_duration * 5:
                    # 落后超过 5 帧，重置基准时间防止追赶导致爆发写入
                    logger.warning(f"[SYNC] Pacer: Behind by {-wait*1000:.0f}ms, resetting time base")
                    start_time = time.perf_counter() - tick * frame_duration

                # 3. 取视频帧
                video_bytes = None
                with session._video_queue_lock:
                    if session._video_queue:
                        video_bytes = session._video_queue.popleft()
                    vq_len = len(session._video_queue)

                if video_bytes is None:
                    # 没有视频帧，使用上一帧（保持时间线连续）
                    video_bytes = session._last_video_bytes

                if video_bytes is None:
                    # 完全没有视频数据，跳过这个 tick
                    tick += 1
                    continue

                # 4. 音频缓冲区对齐：保持和视频队列同样长度
                #    视频队列丢弃旧帧，音频缓冲区必须同步丢弃旧数据，
                #    否则 pacer 会把旧音频配新视频 → 声音落后
                with session._audio_buffer_lock:
                    max_audio_bytes = (vq_len + 2) * audio_bytes_per_frame
                    if len(session._audio_buffer) > max_audio_bytes:
                        excess = len(session._audio_buffer) - max_audio_bytes
                        # 按 audio_bytes_per_frame 对齐裁剪，避免截断采样
                        excess = (excess // audio_bytes_per_frame) * audio_bytes_per_frame
                        if excess > 0:
                            del session._audio_buffer[:excess]

                # 5. 检查音频写入器是否可用
                with session.state_lock:
                    writer = session.audio_writer

                if writer is None:
                    # 音频连接断开，不能只写视频不写音频（否则 ffmpeg PTS 偏移）
                    # 跳过本 tick，等待音频连接恢复
                    # 不递增 tick，让时间基准自然对齐
                    session._last_video_bytes = video_bytes
                    continue

                # 6. 取音频数据（此时 writer 一定不为 None）
                audio_bytes = None
                with session._audio_buffer_lock:
                    if len(session._audio_buffer) >= audio_bytes_per_frame:
                        audio_bytes = bytes(session._audio_buffer[:audio_bytes_per_frame])
                        del session._audio_buffer[:audio_bytes_per_frame]
                    elif len(session._audio_buffer) > 0:
                        # 缓冲不足一帧，取现有数据 + 静音填充
                        available = len(session._audio_buffer)
                        audio_bytes = bytes(session._audio_buffer) + silence_audio[available:]
                        session._audio_buffer.clear()

                if audio_bytes is None:
                    # 没有音频数据，填充静音
                    audio_bytes = silence_audio
                    consecutive_silence += 1
                else:
                    consecutive_silence = 0

                # 7. 检查 ffmpeg 进程和视频写入错误
                if video_write_error[0]:
                    logger.error("[SYNC] Pacer: Video writer encountered error, stopping")
                    break
                if session.ffmpeg_process is None or session.ffmpeg_process.poll() is not None:
                    logger.error("[SYNC] Pacer: ffmpeg 进程已退出")
                    break

                # 8. 投递视频帧到写线程（非阻塞）
                try:
                    video_write_queue.put_nowait((video_bytes, tick))
                except queue.Full:
                    # 写线程来不及消费，跳过这帧
                    pass

                # 9. 写入音频 (TCP/FIFO)
                try:
                    with session.audio_write_lock:
                        if isinstance(writer, socket.socket):
                            writer.sendall(audio_bytes)
                        elif isinstance(writer, int):
                            os.write(writer, audio_bytes)
                except (BrokenPipeError, OSError, ConnectionError):
                    pass  # 下一个 tick 会检测到 writer 变化

                # 10. 更新状态
                session.frame_count += 1
                session.audio_sample_count += samples_per_frame
                session._last_video_bytes = video_bytes
                tick += 1

                # 11. 定期日志
                if session.frame_count == 1:
                    logger.info("[SYNC] Pacer: First synced frame+audio written to ffmpeg")
                if session.frame_count % 500 == 0:
                    audio_dur = session.audio_sample_count / sample_rate
                    video_dur = session.frame_count / fps
                    buf_len = len(session._audio_buffer)
                    logger.info(f"[SYNC] Pacer: frame={session.frame_count} "
                               f"video_dur={video_dur:.1f}s audio_dur={audio_dur:.1f}s "
                               f"video_q={vq_len} audio_buf={buf_len}")

            logger.info("[SYNC] Pacer loop ended, cleaning up video writer thread")
            # 停止视频写线程
            try:
                video_write_queue.put(None, timeout=1.0)
            except queue.Full:
                pass
            session._pacer_quit.set()
            vw_thread.join(timeout=2.0)
            logger.info("[SYNC] Pacer stopped")

        thread = threading.Thread(target=pacer_loop, daemon=True, name="srt-sync-pacer")
        session._pacer_start_time = time.time()
        thread.start()
        session._pacer_thread = thread

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

        # 启动同步节奏器
        self._start_sync_pacer(session, config)

        def open_audio_fifo():
            try:
                fd = os.open(audio_fifo, os.O_WRONLY)
                with session.state_lock:
                    session.audio_writer = fd
                    dropped_chunks = len(session._pre_connect_audio_buffer)
                    dropped_bytes = sum(len(b) for b in session._pre_connect_audio_buffer)
                    session._pre_connect_audio_buffer.clear()
                if dropped_chunks > 0:
                    logger.info(f"[SYNC] SRT: 音频 FIFO 已打开，丢弃 {dropped_chunks} 块旧缓冲 "
                               f"({dropped_bytes} bytes)")
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

        # 启动同步节奏器
        self._start_sync_pacer(session, config)

        def accept_audio_connection():
            try:
                server_sock.settimeout(15)
                conn, _ = server_sock.accept()
                with session.state_lock:
                    session.audio_writer = conn
                    dropped_chunks = len(session._pre_connect_audio_buffer)
                    dropped_bytes = sum(len(b) for b in session._pre_connect_audio_buffer)
                    session._pre_connect_audio_buffer.clear()
                if dropped_chunks > 0:
                    logger.info(f"[SYNC] SRT: 音频 TCP 连接已建立，丢弃 {dropped_chunks} 块旧缓冲 "
                               f"({dropped_bytes} bytes)")
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
            # 检查节奏器线程是否存活
            if context.session._pacer_thread is not None and not context.session._pacer_thread.is_alive():
                logger.error("SRT: Sync pacer thread died, resetting session")
                self._record_failure(context)
                context.session.reset()
                context.session = None
            # 检查 pacer 是否卡死（线程活着但 frame_count=0 超过 35 秒）
            elif context.session.frame_count == 0 and context.session._pacer_start_time > 0:
                stuck_duration = time.time() - context.session._pacer_start_time
                if stuck_duration > 35.0:
                    logger.error(f"SRT: Pacer stuck for {stuck_duration:.0f}s with 0 frames, resetting session")
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
        logger.info(f"SRT: ffmpeg session started, pid={session.ffmpeg_process.pid if session.ffmpeg_process else 'None'}")
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
        """将视频帧放入队列，由同步节奏器线程消费"""
        if not session.is_running:
            return False

        if video_frame is None or video_frame.size == 0:
            return True

        try:
            rgb_bytes = self._prepare_video_frame(video_frame)

            with session._video_queue_lock:
                # 限制队列长度，超出则丢弃最旧的帧
                max_size = self.config.video_queue_size if self.config else 30
                q_len = len(session._video_queue)
                while q_len >= max_size:
                    session._video_queue.popleft()
                    q_len -= 1
                session._video_queue.append(rgb_bytes)
                q_len += 1

            if session.frame_count == 0 and q_len <= 3:
                logger.info(f"[SYNC] SRT: Video frame queued ({len(rgb_bytes)} bytes, queue={q_len})")

            return True

        except Exception as e:
            logger.error(f"SRT: Failed to queue video frame: {e}")
            return False

    def _write_audio(self, session: SRTSession, audio_data: np.ndarray):
        """将音频数据追加到缓冲区，由同步节奏器线程消费"""
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

            with session._audio_buffer_lock:
                # 限制缓冲区大小（最多 5 秒）
                max_bytes = self.config.audio_sample_rate * 4 * 5 if self.config else 24000 * 4 * 5
                if len(session._audio_buffer) + len(audio_bytes) > max_bytes:
                    # 丢弃最旧的数据
                    excess = len(session._audio_buffer) + len(audio_bytes) - max_bytes
                    del session._audio_buffer[:excess]
                session._audio_buffer.extend(audio_bytes)

            session._audio_buffer_event.set()

            # 首次音频写入日志
            with session._audio_buffer_lock:
                buf_samples = len(session._audio_buffer) // 4
            if session.audio_sample_count == 0 and buf_samples <= 2880:  # 只前3帧
                logger.info(f"[SYNC] SRT: Audio buffered, {len(audio_data)} samples, "
                           f"total buffer={buf_samples} samples")

            # 如果 TCP/FIFO 还没连接，也暂存一份到 pre_connect buffer
            with session.state_lock:
                if session.audio_writer is None:
                    if len(session._pre_connect_audio_buffer) < 100:
                        session._pre_connect_audio_buffer.append(audio_bytes)

            return True

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

        if inputs.type == ChatDataType.AVATAR_VIDEO:
            video_frame = inputs.data.get_main_data()
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
