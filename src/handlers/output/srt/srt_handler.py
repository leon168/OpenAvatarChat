"""
SRT 推流输出 Handler

将 Avatar 的视频和音频输出通过 SRT 协议推送到 SRS 服务器
依赖: ffmpeg (需支持 libsrt)

SRS SRT 配置:
  srt_server {
      enabled on;
      listen 10080;
  }

推流 URL 格式:
  srt://127.0.0.1:10080?streamid=#!::r=live/avatar,m=publish
"""

import os
import subprocess
import threading
import numpy as np
from dataclasses import dataclass, field
from typing import Dict, Optional, cast, Any
from pathlib import Path

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
    audio_sample_rate: int = Field(default=16000, description="音频输入采样率")
    audio_bitrate: int = Field(default=128, description="音频码率(kbps)")
    preset: str = Field(default="fast", description="x264 预设 (ultrafast, superfast, veryfast, faster, fast, medium)")
    tune: str = Field(default="zerolatency", description="x264 tune 参数")
    ffmpeg_path: str = Field(default="ffmpeg", description="ffmpeg 可执行文件路径")
    # 环境变量覆盖
    srt_url_env: str = Field(default="SRT_URL", description="SRT URL 环境变量名")


@dataclass
class SRTSession:
    """SRT 会话状态"""
    input_stream_id: ChatStreamIdentity
    output_stream_key: Optional[StreamKey] = None
    ffmpeg_process: Optional[subprocess.Popen] = None
    video_buffer: bytearray = field(default_factory=bytearray)
    audio_buffer: bytearray = field(default_factory=bytearray)
    frame_count: int = 0
    audio_sample_count: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)
    
    def reset(self):
        """重置会话"""
        with self.lock:
            if self.ffmpeg_process is not None:
                try:
                    self.ffmpeg_process.stdin.close()
                    self.ffmpeg_process.wait(timeout=5)
                except Exception:
                    try:
                        self.ffmpeg_process.kill()
                    except Exception:
                        pass
                self.ffmpeg_process = None
            self.video_buffer.clear()
            self.audio_buffer.clear()
            self.frame_count = 0
            self.audio_sample_count = 0


class SRTOutputContext(HandlerContext):
    """SRT 输出 Handler 上下文"""
    
    def __init__(self, session_id: str):
        super().__init__(session_id)
        self.config: Optional[SRTOutputConfig] = None
        self.sessions: Dict[StreamKey, SRTSession] = {}
        self.current_session: Optional[SRTSession] = None
        
    def _create_session(self, input_stream: ChatStreamIdentity) -> SRTSession:
        return SRTSession(input_stream_id=input_stream)


class HandlerSRTOutput(HandlerBase):
    """
    SRT 推流输出 Handler
    
    接收 Avatar 的视频帧和音频数据，通过 ffmpeg 编码并推流到 SRS
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
    
    def _start_ffmpeg(self, config: SRTOutputConfig) -> subprocess.Popen:
        """启动 ffmpeg 进程进行 SRT 推流"""
        # 构建 SRT URL (添加 latency 参数)
        srt_url = config.srt_url
        if "latency=" not in srt_url:
            separator = "&" if "?" in srt_url.split("#")[-1] else "&"
            srt_url = f"{srt_url}{separator}latency={config.latency_ms * 1000}"  # 微秒
        
        command = [
            config.ffmpeg_path,
            "-y",  # 覆盖输出
            "-hide_banner",  # 隐藏版本信息
            "-loglevel", "warning",  # 只显示警告及以上
            # 视频输入 (raw RGB)
            "-f", "rawvideo",
            "-pix_fmt", "rgb24",
            "-s", f"{config.video_width}x{config.video_height}",
            "-r", str(config.fps),
            "-thread_queue_size", "512",  # 输入队列大小
            "-i", "-",  # 从 stdin 读取视频
            # 音频输入 (PCM float32)
            "-f", "f32le",
            "-ar", str(config.audio_sample_rate),
            "-ac", "1",
            "-thread_queue_size", "512",
            "-i", "-",  # 从 stdin 读取音频
            # 视频编码
            "-c:v", "libx264",
            "-preset", config.preset,
            "-tune", config.tune,
            "-b:v", f"{config.video_bitrate}k",
            "-pix_fmt", "yuv420p",
            "-g", str(config.fps * 2),  # GOP 大小
            "-keyint_min", str(config.fps),  # 最小关键帧间隔
            "-sc_threshold", "0",  # 禁用场景切换检测
            # 音频编码
            "-c:a", "aac",
            "-b:a", f"{config.audio_bitrate}k",
            "-ar", "44100",  # AAC 标准采样率
            "-ac", "2",  # 立体声
            # 输出格式
            "-f", "mpegts",  # TS 容器适合 SRT
            "-flush_packets", "1",  # 立即刷新包
            srt_url
        ]
        
        logger.info(f"Starting ffmpeg: {' '.join(command)}")
        
        process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0  # 无缓冲
        )
        
        # 启动 stderr 读取线程 (用于错误日志)
        def read_stderr():
            while process.poll() is None:
                try:
                    line = process.stderr.readline()
                    if line:
                        logger.warning(f"ffmpeg: {line.decode('utf-8', errors='ignore').strip()}")
                except Exception:
                    break
        
        threading.Thread(target=read_stderr, daemon=True).start()
        
        return process
    
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
        outputs = []  # 输出到外部，不产生内部输出
        
        return HandlerDetail(
            inputs=inputs,
            outputs=outputs,
            signal_filters=[
                SignalFilterRule(ChatSignalType.STREAM_CANCEL, None, None)
            ]
        )
    
    def _write_frame_to_ffmpeg(self, session: SRTSession, video_frame: np.ndarray, audio_data: np.ndarray):
        """写入音视频帧到 ffmpeg"""
        if session.ffmpeg_process is None or session.ffmpeg_process.poll() is not None:
            return False
        
        try:
            with session.lock:
                # 写入视频帧 (RGB -> bytes)
                if video_frame is not None and video_frame.size > 0:
                    # 确保格式正确
                    if video_frame.dtype != np.uint8:
                        video_frame = (video_frame * 255).clip(0, 255).astype(np.uint8)
                    
                    # 调整尺寸
                    if video_frame.shape[:2] != (self.config.video_height, self.config.video_width):
                        import cv2
                        video_frame = cv2.resize(video_frame, (self.config.video_width, self.config.video_height))
                    
                    # 转换为 RGB bytes
                    if len(video_frame.shape) == 3 and video_frame.shape[2] == 3:
                        rgb_bytes = video_frame.tobytes()
                        session.ffmpeg_process.stdin.write(rgb_bytes)
                        session.frame_count += 1
                
                # 写入音频 (float32 -> bytes)
                if audio_data is not None and audio_data.size > 0:
                    # 转换为 float32
                    if audio_data.dtype != np.float32:
                        audio_data = audio_data.astype(np.float32)
                    
                    # 确保是 1D 数组
                    audio_data = audio_data.flatten()
                    
                    session.ffmpeg_process.stdin.write(audio_data.tobytes())
                    session.audio_sample_count += len(audio_data)
                
            return True
            
        except BrokenPipeError:
            logger.error("ffmpeg stdin pipe broken")
            return False
        except Exception as e:
            logger.error(f"写入 ffmpeg 失败: {e}")
            return False
    
    def handle(self, context: HandlerContext, inputs: ChatData,
               output_definitions: Dict[ChatDataType, HandlerDataInfo]):
        """处理输入数据"""
        context = cast(SRTOutputContext, context)
        
        input_stream = inputs.stream_id
        input_stream_key = input_stream.key
        
        # 获取或创建会话
        session = context.sessions.get(input_stream_key)
        if session is None:
            # 新的输入流，取消旧的
            for old_key, old_session in list(context.sessions.items()):
                logger.info(f"SRT Output: 取消旧会话 {old_key}")
                old_session.reset()
            context.sessions.clear()
            
            # 创建新会话
            session = context._create_session(input_stream)
            context.sessions[input_stream_key] = session
            context.current_session = session
            
            # 启动 ffmpeg
            try:
                session.ffmpeg_process = self._start_ffmpeg(context.config)
                logger.info(f"SRT Output: ffmpeg 已启动，推流到 {context.config.srt_url}")
            except Exception as e:
                logger.error(f"SRT Output: 启动 ffmpeg 失败: {e}")
                context.sessions.pop(input_stream_key, None)
                return
        
        # 处理数据
        if inputs.type == ChatDataType.AVATAR_VIDEO:
            # 视频帧
            video_frame = inputs.data.get_main_data()
            if video_frame is not None:
                self._write_frame_to_ffmpeg(session, video_frame, None)
                
        elif inputs.type == ChatDataType.AVATAR_AUDIO:
            # 音频数据
            audio_data = inputs.data.get_main_data()
            is_last = inputs.is_last_data
            
            if audio_data is not None:
                self._write_frame_to_ffmpeg(session, None, audio_data)
            
            # 如果是最后一帧，关闭会话
            if is_last:
                logger.info(f"SRT Output: 流结束，关闭会话 {input_stream_key}")
                session.reset()
                context.sessions.pop(input_stream_key, None)
                if context.current_session == session:
                    context.current_session = None
    
    def on_signal(self, context: HandlerContext, signal: ChatSignal):
        """处理打断信号"""
        context = cast(SRTOutputContext, context)
        
        if signal.type == ChatSignalType.STREAM_CANCEL and signal.related_stream:
            stream_key = signal.related_stream.key
            if stream_key is None:
                return
            
            # 检查输入流
            session = context.sessions.pop(stream_key, None)
            if session:
                logger.info(f"SRT Output: 取消会话 {stream_key}")
                session.reset()
                return
    
    def destroy_context(self, context: HandlerContext):
        """销毁上下文"""
        context = cast(SRTOutputContext, context)
        logger.info("SRT Output: 销毁上下文")
        
        for session in context.sessions.values():
            try:
                session.reset()
            except Exception as e:
                logger.warning(f"重置会话失败: {e}")
        
        context.sessions.clear()


# 导入 os 用于环境变量检查
import os
