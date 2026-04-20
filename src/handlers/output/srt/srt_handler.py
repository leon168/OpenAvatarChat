"""
SRT 推流输出 Handler

将 Avatar 的视频和音频输出通过 SRT 协议推送到 SRS 服务器
依赖: ffmpeg (需支持 libsrt)

重要: ffmpeg 不支持从单个 stdin 同时读取两种 raw 格式输入，
因此视频通过 stdin 管道传输，音频通过命名管道(FIFO)传输。

SRS SRT 配置:
  srt_server {
      enabled on;
      listen 10080;
  }

推流 URL 格式:
  srt://127.0.0.1:10080?streamid=#!::r=live/avatar,m=publish
"""

import os
import platform
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
    preset: str = Field(default="fast", description="x264 预设 (ultrafast, superfast, veryfast, faster, fast, medium)")
    tune: str = Field(default="zerolatency", description="x264 tune 参数")
    ffmpeg_path: str = Field(default="ffmpeg", description="ffmpeg 可执行文件路径")
    srt_url_env: str = Field(default="SRT_URL", description="SRT URL 环境变量名")


@dataclass
class SRTSession:
    """SRT 会话状态 - 管理单个 ffmpeg 进程

    视频通过 subprocess stdin 传输，音频通过 FIFO/命名管道 传输。
    """
    ffmpeg_process: Optional[subprocess.Popen] = None
    audio_writer: Optional[object] = None  # FIFO fd 或 TCP socket
    audio_fifo_path: Optional[str] = None
    tmp_dir: Optional[str] = None
    frame_count: int = 0
    audio_sample_count: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)
    active_streams: Set[str] = field(default_factory=set)
    _audio_server_socket: Optional[socket.socket] = None

    def reset(self):
        """重置会话，关闭 ffmpeg 进程和音频管道"""
        with self.lock:
            # 关闭音频写入端
            if self.audio_writer is not None:
                try:
                    if isinstance(self.audio_writer, socket.socket):
                        self.audio_writer.close()
                    elif isinstance(self.audio_writer, int):
                        os.close(self.audio_writer)
                except Exception:
                    pass
                self.audio_writer = None

            # 关闭音频服务端 socket
            if self._audio_server_socket is not None:
                try:
                    self._audio_server_socket.close()
                except Exception:
                    pass
                self._audio_server_socket = None

            # 关闭 ffmpeg
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

            # 清理 FIFO 文件
            if self.audio_fifo_path and os.path.exists(self.audio_fifo_path):
                try:
                    os.unlink(self.audio_fifo_path)
                except Exception:
                    pass
                self.audio_fifo_path = None

            # 清理临时目录
            if self.tmp_dir and os.path.exists(self.tmp_dir):
                try:
                    os.rmdir(self.tmp_dir)
                except Exception:
                    pass
                self.tmp_dir = None

            self.frame_count = 0
            self.audio_sample_count = 0
            self.active_streams.clear()

    @property
    def is_running(self) -> bool:
        return self.ffmpeg_process is not None and self.ffmpeg_process.poll() is None


class SRTOutputContext(HandlerContext):
    """SRT 输出 Handler 上下文"""

    def __init__(self, session_id: str):
        super().__init__(session_id)
        self.config: Optional[SRTOutputConfig] = None
        self.session: Optional[SRTSession] = None


class HandlerSRTOutput(HandlerBase):
    """
    SRT 推流输出 Handler

    接收 Avatar 的视频帧和音频数据，通过 ffmpeg 编码并推流到 SRS。
    视频通过 stdin 管道传输，音频通过命名管道(FIFO)传输，
    解决 ffmpeg 无法从单个 stdin 同时读取两种 raw 格式输入的问题。
    """

    def __init__(self):
        super().__init__()
        self.config: Optional[SRTOutputConfig] = None

    def get_handler_info(self) -> HandlerBaseInfo:
        return HandlerBaseInfo(
            config_model=SRTOutputConfig,
        )

    def load(self, engine_config: ChatEngineConfigModel, handler_config: Optional[BaseModel] = None):
        self.config = cast(SRTOutputConfig, handler_config or SRTOutputConfig())

        # 优先使用环境变量
        if self.config.srt_url_env in os.environ:
            self.config.srt_url = os.getenv(self.config.srt_url_env, self.config.srt_url)

        # 检查 ffmpeg
        try:
            result = subprocess.run(
                [self.config.ffmpeg_path, "-protocols"],
                capture_output=True,
                text=True,
                timeout=5
            )
            if "srt" not in result.stdout:
                logger.warning(f"ffmpeg 可能不支持 SRT 协议，请检查编译选项: {self.config.ffmpeg_path}")
            else:
                logger.info(f"ffmpeg SRT 支持已确认: {self.config.ffmpeg_path}")
        except Exception as e:
            logger.error(f"检查 ffmpeg 失败: {e}")

        logger.info(f"SRT Output Handler loaded, url={self.config.srt_url}, "
                   f"latency={self.config.latency_ms}ms, "
                   f"video={self.config.video_width}x{self.config.video_height}@{self.config.fps}fps")

    def _build_srt_url(self, config: SRTOutputConfig) -> str:
        """构建带 latency 参数的 SRT URL"""
        srt_url = config.srt_url
        if "latency=" not in srt_url:
            srt_url = f"{srt_url}&latency={config.latency_ms * 1000}"
        return srt_url

    def _start_ffmpeg_with_fifo(self, config: SRTOutputConfig) -> SRTSession:
        """Linux/macOS: 使用 FIFO 命名管道传输音频"""
        # 创建临时目录和音频 FIFO
        tmp_dir = tempfile.mkdtemp(prefix='srt_')
        audio_fifo = os.path.join(tmp_dir, 'audio')
        os.mkfifo(audio_fifo)

        srt_url = self._build_srt_url(config)

        command = [
            config.ffmpeg_path,
            "-y", "-hide_banner", "-loglevel", "warning",
            # 视频输入 (raw RGB from stdin)
            "-f", "rawvideo", "-pix_fmt", "rgb24",
            "-s", f"{config.video_width}x{config.video_height}",
            "-r", str(config.fps),
            "-thread_queue_size", "512",
            "-i", "-",
            # 音频输入 (f32le from FIFO)
            "-f", "f32le", "-ar", str(config.audio_sample_rate), "-ac", "1",
            "-thread_queue_size", "512",
            "-i", audio_fifo,
            # 视频编码
            "-c:v", "libx264",
            "-preset", config.preset,
            "-tune", config.tune,
            "-b:v", f"{config.video_bitrate}k",
            "-pix_fmt", "yuv420p",
            "-g", str(config.fps * 2),
            "-keyint_min", str(config.fps),
            "-sc_threshold", "0",
            # 音频编码
            "-c:a", "aac",
            "-b:a", f"{config.audio_bitrate}k",
            "-ar", "44100",
            "-ac", "2",
            # 输出格式
            "-f", "mpegts",
            "-flush_packets", "1",
            srt_url
        ]

        logger.info(f"Starting ffmpeg: {' '.join(command)}")

        process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0
        )

        # 启动 stderr 读取线程
        self._start_stderr_reader(process)

        # 在单独线程中打开 FIFO 写入端 (避免死锁)
        # ffmpeg 读取端会阻塞直到写入端打开
        audio_fd_result = [None]
        audio_fd_error = [None]
        audio_fd_ready = threading.Event()

        def open_audio_fifo():
            try:
                audio_fd_result[0] = os.open(audio_fifo, os.O_WRONLY)
            except Exception as e:
                audio_fd_error[0] = e
            finally:
                audio_fd_ready.set()

        threading.Thread(target=open_audio_fifo, daemon=True).start()

        # 等待 FIFO 打开，同时检查 ffmpeg 进程状态
        # 如果 ffmpeg 在等待期间崩溃，不需要继续等待
        start_time = time.time()
        timeout = 10
        while not audio_fd_ready.is_set():
            elapsed = time.time() - start_time
            if elapsed >= timeout:
                break
            # 检查 ffmpeg 是否已退出
            if process.poll() is not None:
                # ffmpeg 已退出，取消 FIFO 等待
                retcode = process.poll()
                logger.error(f"SRT: ffmpeg exited prematurely with code {retcode}")
                # 尝试读取 stderr 中的错误信息
                try:
                    stderr_output = process.stderr.read().decode('utf-8', errors='ignore')
                    if stderr_output:
                        logger.error(f"SRT: ffmpeg stderr: {stderr_output[:500]}")
                except Exception:
                    pass
                break
            audio_fd_ready.wait(timeout=0.5)

        if audio_fd_error[0] is not None:
            process.kill()
            raise RuntimeError(f"无法打开音频 FIFO: {audio_fd_error[0]}")

        audio_fd = audio_fd_result[0]
        if audio_fd is None:
            process.kill()
            # 提供更具体的错误信息
            if process.poll() is not None:
                raise RuntimeError(f"ffmpeg 启动后立即退出 (code={process.poll()})，无法打开音频 FIFO")
            raise RuntimeError("打开音频 FIFO 超时")

        session = SRTSession()
        session.ffmpeg_process = process
        session.audio_writer = audio_fd
        session.audio_fifo_path = audio_fifo
        session.tmp_dir = tmp_dir

        logger.info(f"SRT Output: ffmpeg 已启动 (FIFO 模式)，推流到 {srt_url}")
        return session

    def _start_ffmpeg_with_tcp(self, config: SRTOutputConfig) -> SRTSession:
        """Windows: 使用 TCP socket 传输音频"""
        # 创建 TCP 服务端 socket
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind(('127.0.0.1', 0))
        _, audio_port = server_sock.getsockname()
        server_sock.listen(1)

        srt_url = self._build_srt_url(config)
        audio_url = f"tcp://127.0.0.1:{audio_port}"

        command = [
            config.ffmpeg_path,
            "-y", "-hide_banner", "-loglevel", "warning",
            # 视频输入 (raw RGB from stdin)
            "-f", "rawvideo", "-pix_fmt", "rgb24",
            "-s", f"{config.video_width}x{config.video_height}",
            "-r", str(config.fps),
            "-thread_queue_size", "512",
            "-i", "-",
            # 音频输入 (f32le from TCP)
            "-f", "f32le", "-ar", str(config.audio_sample_rate), "-ac", "1",
            "-thread_queue_size", "512",
            "-i", audio_url,
            # 视频编码
            "-c:v", "libx264",
            "-preset", config.preset,
            "-tune", config.tune,
            "-b:v", f"{config.video_bitrate}k",
            "-pix_fmt", "yuv420p",
            "-g", str(config.fps * 2),
            "-keyint_min", str(config.fps),
            "-sc_threshold", "0",
            # 音频编码
            "-c:a", "aac",
            "-b:a", f"{config.audio_bitrate}k",
            "-ar", "44100",
            "-ac", "2",
            # 输出格式
            "-f", "mpegts",
            "-flush_packets", "1",
            srt_url
        ]

        logger.info(f"Starting ffmpeg: {' '.join(command)}")

        process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0
        )

        # 启动 stderr 读取线程
        self._start_stderr_reader(process)

        # 等待 ffmpeg 连接到 TCP socket
        server_sock.settimeout(10)
        try:
            conn, _ = server_sock.accept()
        except socket.timeout:
            process.kill()
            raise RuntimeError("等待 ffmpeg 连接音频 TCP 超时")

        session = SRTSession()
        session.ffmpeg_process = process
        session.audio_writer = conn
        session._audio_server_socket = server_sock

        logger.info(f"SRT Output: ffmpeg 已启动 (TCP 模式)，推流到 {srt_url}")
        return session

    def _start_stderr_reader(self, process: subprocess.Popen):
        """启动 stderr 读取线程"""
        def read_stderr():
            while process.poll() is None:
                try:
                    line = process.stderr.readline()
                    if line:
                        logger.warning(f"ffmpeg: {line.decode('utf-8', errors='ignore').strip()}")
                except Exception:
                    break

        threading.Thread(target=read_stderr, daemon=True).start()

    def _start_ffmpeg(self, config: SRTOutputConfig) -> SRTSession:
        """启动 ffmpeg 进程，根据平台选择音频传输方式"""
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
        inputs = [
            HandlerDataInfo(type=ChatDataType.AVATAR_VIDEO),
            HandlerDataInfo(type=ChatDataType.AVATAR_AUDIO),
        ]
        outputs = []

        return HandlerDetail(
            inputs=inputs,
            outputs=outputs,
            signal_filters=[
                SignalFilterRule(ChatSignalType.STREAM_CANCEL, None, None)
            ]
        )

    def _ensure_session(self, context: SRTOutputContext) -> SRTSession:
        """确保有一个活跃的 ffmpeg 会话"""
        if context.session is not None and context.session.is_running:
            return context.session

        # 清理旧会话
        if context.session is not None:
            context.session.reset()
            context.session = None

        try:
            session = self._start_ffmpeg(context.config)
        except Exception as e:
            logger.error(f"SRT Output: 启动 ffmpeg 失败: {e}")
            return None

        context.session = session
        return session

    def _write_video(self, session: SRTSession, video_frame: np.ndarray):
        """写入视频帧到 ffmpeg stdin"""
        if not session.is_running:
            return False

        if video_frame is None:
            logger.debug("SRT: video frame is None, skipping")
            return True

        if video_frame.size == 0:
            logger.warning(f"SRT: video frame size is 0, shape: {video_frame.shape}")
            return True

        try:
            # 去除批次维度 (1, H, W, 3) -> (H, W, 3)
            if video_frame.ndim == 4 and video_frame.shape[0] == 1:
                video_frame = video_frame.squeeze(axis=0)

            # 确保格式正确 (uint8)
            if video_frame.dtype != np.uint8:
                video_frame = (video_frame * 255).clip(0, 255).astype(np.uint8)

            # 确保是 3 通道
            if video_frame.ndim == 2:
                video_frame = np.stack([video_frame] * 3, axis=-1)
            elif video_frame.shape[2] == 4:
                video_frame = video_frame[:, :, :3]

            # BGR -> RGB (LiteAvatar 输出 BGR, ffmpeg 需要 rgb24)
            video_frame = video_frame[:, :, ::-1]

            # 调整尺寸
            target_width = self.config.video_width if self.config.video_width > 0 else 512
            target_height = self.config.video_height if self.config.video_height > 0 else 512

            if video_frame.shape[0] != target_height or video_frame.shape[1] != target_width:
                import cv2
                video_frame = cv2.resize(video_frame, (target_width, target_height))

            # 写入 stdin
            with session.lock:
                if session.ffmpeg_process is None or session.ffmpeg_process.poll() is not None:
                    return False
                session.ffmpeg_process.stdin.write(video_frame.tobytes())
                session.frame_count += 1

                if session.frame_count % 25 == 0:
                    logger.info(f"SRT: Sent {session.frame_count} video frames")

            return True

        except BrokenPipeError:
            logger.error("SRT: ffmpeg stdin pipe broken (video)")
            return False
        except Exception as e:
            logger.error(f"SRT: 写入视频帧失败: {e}")
            return False

    def _write_audio(self, session: SRTSession, audio_data: np.ndarray):
        """写入音频数据到音频管道 (FIFO 或 TCP socket)

        LiteAvatar 输出 int16 PCM, shape (1, N), 24000Hz。
        ffmpeg 期望 f32le 格式，值域 [-1.0, 1.0]。
        """
        if not session.is_running:
            return False

        if audio_data is None or audio_data.size == 0:
            return True

        try:
            # 展平为 1D 数组 (处理 (1, N) shape)
            audio_data = audio_data.flatten()

            # int16 -> 归一化 float32 [-1.0, 1.0]
            if audio_data.dtype == np.int16:
                audio_data = audio_data.astype(np.float32) / 32768.0
            elif audio_data.dtype != np.float32:
                audio_data = audio_data.astype(np.float32)
                max_val = np.abs(audio_data).max()
                if max_val > 1.0:
                    audio_data = audio_data / max_val

            audio_bytes = audio_data.tobytes()

            with session.lock:
                if session.audio_writer is None:
                    return False

                if isinstance(session.audio_writer, socket.socket):
                    session.audio_writer.sendall(audio_bytes)
                elif isinstance(session.audio_writer, int):
                    os.write(session.audio_writer, audio_bytes)

                session.audio_sample_count += len(audio_data)

            return True

        except (BrokenPipeError, OSError, ConnectionError) as e:
            logger.error(f"SRT: 写入音频数据失败: {e}")
            return False
        except Exception as e:
            logger.error(f"SRT: 写入音频数据失败: {e}")
            return False

    def handle(self, context: HandlerContext, inputs: ChatData,
               output_definitions: Dict[ChatDataType, HandlerDataInfo]):
        """处理输入数据 - 音视频流共享同一个 ffmpeg 进程"""
        context = cast(SRTOutputContext, context)

        input_stream = inputs.stream_id
        stream_key_str = str(input_stream.key) if input_stream and input_stream.key else "unknown"

        # 确保有活跃的 ffmpeg 会话
        session = self._ensure_session(context)
        if session is None:
            logger.error("SRT Output: 无法创建 ffmpeg 会话")
            return

        # 记录活跃流
        stream_id_str = f"{inputs.type.value}:{stream_key_str}"
        if stream_id_str not in session.active_streams:
            session.active_streams.add(stream_id_str)
            logger.info(f"SRT Output: 新流加入 {stream_id_str}, 活跃流: {session.active_streams}")

        # 根据数据类型分发处理
        if inputs.type == ChatDataType.AVATAR_VIDEO:
            video_frame = inputs.data.get_main_data()
            if not self._write_video(session, video_frame):
                # 写入失败，重置会话
                session.reset()
                context.session = None

        elif inputs.type == ChatDataType.AVATAR_AUDIO:
            audio_data = inputs.data.get_main_data()
            if not self._write_audio(session, audio_data):
                # 写入失败，重置会话
                session.reset()
                context.session = None

            # 如果是最后一帧音频，检查是否需要关闭会话
            if inputs.is_last_data:
                session.active_streams.discard(stream_id_str)
                logger.info(f"SRT Output: 音频流结束 {stream_id_str}, 剩余活跃流: {session.active_streams}")

                if not session.active_streams:
                    logger.info("SRT Output: 所有流结束，关闭 ffmpeg 会话")
                    session.reset()
                    context.session = None

    def on_signal(self, context: HandlerContext, signal: ChatSignal):
        """处理打断信号"""
        context = cast(SRTOutputContext, context)

        if signal.type == ChatSignalType.STREAM_CANCEL:
            logger.info("SRT Output: 收到取消信号，关闭 ffmpeg 会话")
            if context.session is not None:
                context.session.reset()
                context.session = None

    def destroy_context(self, context: HandlerContext):
        """销毁上下文"""
        context = cast(SRTOutputContext, context)
        logger.info("SRT Output: 销毁上下文")

        if context.session is not None:
            try:
                context.session.reset()
            except Exception as e:
                logger.warning(f"重置会话失败: {e}")
            context.session = None
