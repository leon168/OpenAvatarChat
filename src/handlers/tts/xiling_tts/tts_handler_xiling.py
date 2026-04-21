"""
百度智能云流式文本在线合成 TTS Handler

基于 WebSocket 的流式 TTS，支持 API Key 鉴权
文档: https://cloud.baidu.com/doc/SPEECH/s/lm5xd63rn
"""

import asyncio
import json
import os
import re
import time
import aiohttp
import threading
from dataclasses import dataclass, field
from typing import Dict, Optional, cast, Any
from urllib.parse import urlencode

import numpy as np
import websockets
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


class XilingTTSConfig(HandlerBaseConfigModel, BaseModel):
    """百度智能云 TTS 配置"""
    auth_type: str = Field(default="api_key", description="鉴权方式: api_key 或 access_token")
    api_key: str = Field(default="", description="API Key (用于 api_key 鉴权)")
    secret_key: str = Field(default="", description="Secret Key (用于获取 access_token)")
    access_token: str = Field(default="", description="直接提供 access_token (用于 access_token 鉴权)")
    per: str = Field(default="5118", description="发音人 ID，默认度小萌")
    spd: int = Field(default=5, description="语速，0-15，默认5")
    pit: int = Field(default=5, description="音调，0-15，默认5")
    vol: int = Field(default=5, description="音量，基础音库取值0-9，其他音库取值 0-15，默认为 5")
    sample_rate: int = Field(default=16000, description="采样率，仅支持16000")
    aue: int = Field(default=4, description="音频格式，3=mp3，4=pcm-16k/24k，5=pcm-8k，6=wav，默认4(pcm，代码按 raw int16 处理)")
    # 超时配置
    connect_timeout: float = Field(default=10.0, description="WebSocket 连接超时时间（秒）")
    init_timeout: float = Field(default=10.0, description="会话初始化超时时间（秒）")
    audio_timeout: float = Field(default=10.0, description="音频数据接收超时时间（秒）")
    audio_finish_timeout: float = Field(default=30.0, description="结束阶段音频接收超时时间（秒）")


@dataclass
class XilingTTSSession:
    """TTS 会话状态"""
    input_stream_id: ChatStreamIdentity
    output_stream_key: Optional[StreamKey] = None
    websocket: Optional[websockets.WebSocketClientProtocol] = None
    cancelled: bool = False
    audio_buffer: bytearray = field(default_factory=bytearray)
    
    def reset(self):
        """重置会话（保留 WebSocket 连接以复用）"""
        self.cancelled = True
        self.audio_buffer = bytearray()
        # 不关闭 WebSocket，留待下次复用
    
    def close_websocket(self):
        """关闭 WebSocket 连接"""
        if self.websocket is not None:
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    asyncio.create_task(self.websocket.close())
                else:
                    loop.run_until_complete(self.websocket.close())
            except Exception:
                try:
                    loop = asyncio.new_event_loop()
                    loop.run_until_complete(self.websocket.close())
                    loop.close()
                except Exception:
                    pass
            self.websocket = None


class XilingTTSContext(HandlerContext):
    """TTS Handler 上下文"""
    
    def __init__(self, session_id: str):
        super().__init__(session_id)
        self.config: Optional[XilingTTSConfig] = None
        self.sessions: Dict[StreamKey, XilingTTSSession] = {}
        self._cached_websocket: Optional[websockets.WebSocketClientProtocol] = None  # WebSocket 复用
        
    def _create_session(self, input_stream: ChatStreamIdentity) -> XilingTTSSession:
        return XilingTTSSession(input_stream_id=input_stream)


class HandlerTTSXiling(HandlerBase):
    """
    百度智能云流式 TTS Handler
    
    使用 WebSocket 协议进行流式语音合成
    """
    
    # WebSocket 连接地址
    WS_URL = "wss://aip.baidubce.com/ws/2.0/speech/publiccloudspeech/v1/tts"
    # Token 获取地址
    TOKEN_URL = "https://aip.baidubce.com/oauth/2.0/token"
    
    def __init__(self):
        super().__init__()
        self.config: Optional[XilingTTSConfig] = None
        self.access_token: Optional[str] = None
        
    def get_handler_info(self) -> HandlerBaseInfo:
        return HandlerBaseInfo(
            config_model=XilingTTSConfig,
        )
    
    def load(self, engine_config: ChatEngineConfigModel, handler_config: Optional[BaseModel] = None):
        self.config = cast(XilingTTSConfig, handler_config or XilingTTSConfig())
        
        # 优先使用环境变量
        if "XILING_API_KEY" in os.environ:
            self.config.api_key = os.getenv("XILING_API_KEY", "")
        if "XILING_SECRET_KEY" in os.environ:
            self.config.secret_key = os.getenv("XILING_SECRET_KEY", "")
        if "XILING_ACCESS_TOKEN" in os.environ:
            self.config.access_token = os.getenv("XILING_ACCESS_TOKEN", "")
            
        self.access_token = self.config.access_token
        
        # 检查认证配置
        if self.config.auth_type == "api_key" and not self.config.api_key:
            logger.warning("Xiling TTS: api_key 为空，语音合成将失败！请配置 api_key 或设置环境变量 XILING_API_KEY")
        elif self.config.auth_type == "access_token" and not self.config.access_token:
            if not self.config.api_key or not self.config.secret_key:
                logger.warning("Xiling TTS: access_token 和 api_key/secret_key 均为空，语音合成将失败！")
        
        logger.info(f"Xiling TTS loaded, per={self.config.per}, sample_rate={self.config.sample_rate}, "
                   f"auth_type={self.config.auth_type}")
    
    async def _get_access_token(self) -> str:
        """使用 API Key 和 Secret Key 获取 access_token
        
        百度鉴权接口: POST https://aip.baidubce.com/oauth/2.0/token?grant_type=client_credentials&client_id=xxx&client_secret=xxx
        """
        if self.access_token:
            return self.access_token
            
        if not self.config.api_key or not self.config.secret_key:
            raise ValueError("API Key 和 Secret Key 不能为空")
        
        # URL 参数
        params = {
            "grant_type": "client_credentials",
            "client_id": self.config.api_key,
            "client_secret": self.config.secret_key
        }
        url = f"{self.TOKEN_URL}?{urlencode(params)}"
        
        # POST 请求，payload 为空
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, data="") as resp:
                result = await resp.json()
                if "access_token" in result:
                    self.access_token = result["access_token"]
                    logger.info(f"获取 access_token 成功，有效期 {result.get('expires_in', 'unknown')} 秒")
                    return self.access_token
                else:
                    raise ValueError(f"获取 access_token 失败: {result}")
    
    async def _connect_websocket(self) -> websockets.WebSocketClientProtocol:
        """建立 WebSocket 连接"""
        params = {
            "per": self.config.per,
        }
        
        # 根据鉴权方式选择参数或 Header
        if self.config.auth_type == "api_key":
            # 鉴权 API Key：使用 Authorization Header
            extra_headers = {
                "Authorization": f"Bearer {self.config.api_key}"
            }
            url = f"{self.WS_URL}?{urlencode(params)}"
            # 添加连接超时
            websocket = await asyncio.wait_for(
                websockets.connect(url, additional_headers=extra_headers),
                timeout=self.config.connect_timeout
            )
            logger.info("Xiling TTS WebSocket 连接成功 (Authorization 鉴权)")
        else:
            # 鉴权令牌：使用 access_token URL 参数
            token = await self._get_access_token()
            params["access_token"] = token
            url = f"{self.WS_URL}?{urlencode(params)}"
            # 添加连接超时
            websocket = await asyncio.wait_for(
                websockets.connect(url),
                timeout=self.config.connect_timeout
            )
            logger.info("Xiling TTS WebSocket 连接成功 (access_token 鉴权)")
        
        return websocket
    
    async def _initialize_session(self, websocket: websockets.WebSocketClientProtocol) -> bool:
        """初始化会话：发送 system.start 并等待 system.started
        
        官方交互流程:
        1. 发送 system.start (包含 spd, pit, vol, aue 等参数)
        2. 等待 system.started 响应
        """
        try:
            # 发送 system.start
            start_payload = {
                "type": "system.start",
                "payload": {
                    "spd": self.config.spd,
                    "pit": self.config.pit,
                    "vol": self.config.vol,
                    "aue": self.config.aue
                }
            }
            await websocket.send(json.dumps(start_payload))
            logger.debug(f"Xiling TTS 发送 system.start: {start_payload}")
            
            # 等待 system.started
            response = await asyncio.wait_for(websocket.recv(), timeout=self.config.init_timeout)
            if isinstance(response, str):
                data = json.loads(response)
                msg_type = data.get("type", "")
                code = data.get("code", -1)
                message = data.get("message", "")
                
                if msg_type == "system.started":
                    if code == 0:
                        session_id = data.get("headers", {}).get("session_id", "")
                        logger.info(f"Xiling TTS 初始化成功: session_id={session_id}")
                        return True
                    else:
                        logger.error(f"Xiling TTS 初始化失败: code={code}, message={message}")
                        return False
                elif msg_type == "system.error":
                    logger.error(f"Xiling TTS 初始化错误: code={code}, message={message}")
                    return False
                else:
                    logger.warning(f"Xiling TTS 未知的初始化响应: {data}")
                    return False
            else:
                logger.error("Xiling TTS 初始化响应格式错误: 期望文本消息，收到二进制")
                return False
                
        except asyncio.TimeoutError:
            logger.error("Xiling TTS 初始化超时")
            return False
        except Exception as e:
            logger.error(f"Xiling TTS 初始化失败: {e}")
            return False
    
    def _filter_text(self, text: str) -> str:
        """过滤文本，移除特殊字符"""
        if not text:
            return ""
        # 移除 SSML 标签
        text = re.sub(r'<\|.*?\|>', "", text)
        # 限制长度（百度限制 1000 字）
        if len(text) > 1000:
            text = text[:1000]
            logger.warning("文本长度超过 1000 字，已截断")
        return text
    
    def create_context(self, session_context: SessionContext, 
                       handler_config: Optional[BaseModel] = None) -> HandlerContext:
        context = XilingTTSContext(session_context.session_info.session_id)
        context.config = cast(XilingTTSConfig, handler_config or self.config)   
        return context
    
    def start_context(self, session_context: SessionContext, handler_context: HandlerContext):
        pass
    
    def get_handler_detail(self, session_context: SessionContext,
                           context: HandlerContext) -> HandlerDetail:
        definition = DataBundleDefinition()
        definition.add_entry(DataBundleEntry.create_audio_entry(
            "avatar_audio", 
            1, 
            self.config.sample_rate if self.config else 16000
        ))
        
        inputs = [
            HandlerDataInfo(type=ChatDataType.AVATAR_TEXT),
        ]
        outputs = [
            HandlerDataInfo(
                type=ChatDataType.AVATAR_AUDIO,
                definition=definition,
            )
        ]
        return HandlerDetail(
            inputs=inputs,
            outputs=outputs,
            signal_filters=[
                SignalFilterRule(ChatSignalType.STREAM_CANCEL, None, None)
            ]
        )
    
    async def _handle_text_stream_async(self, context: XilingTTSContext, data: ChatData):
        """异步处理文本流"""
        input_stream = data.stream_id
        input_stream_key = input_stream.key
        
        session = context.sessions.get(input_stream_key)
        
        if session is None:
            # 新的输入流，取消旧的
            for old_key, old_session in list(context.sessions.items()):
                logger.info(f"Xiling TTS: 取消旧会话 {old_key}")
                old_session.close_websocket()
            context.sessions.clear()
            
            # 创建新会话
            session = context._create_session(input_stream)
            context.sessions[input_stream_key] = session
            
            # 创建输出流
            streamer = context.data_submitter.get_streamer(ChatDataType.AVATAR_AUDIO)
            output_stream_id = streamer.new_stream(
                sources=[session.input_stream_id],
                name="xiling_tts",
                config=ChatStreamConfig(cancelable=True)
            )
            session.output_stream_key = output_stream_id.key
            
            # 尝试复用缓存的 WebSocket 连接
            ws_reused = False
            cached_ws = context._cached_websocket
            if cached_ws is not None:
                try:
                    from websockets.protocol import State
                    ws_open = cached_ws.state == State.OPEN
                except Exception:
                    ws_open = not getattr(cached_ws, 'closed', True)
                if ws_open:
                    try:
                        init_ok = await self._initialize_session(cached_ws)
                        if init_ok:
                            session.websocket = cached_ws
                            context._cached_websocket = None
                            ws_reused = True
                            logger.info("[LATENCY] Xiling TTS: 复用 WebSocket 连接，跳过连接建立 (~250ms saved)")
                    except Exception as e:
                        logger.warning(f"Xiling TTS: 复用连接初始化失败: {e}，重新连接")
                        try:
                            await cached_ws.close()
                        except Exception:
                            pass
                    context._cached_websocket = None
            
            if not ws_reused:
                # 建立新的 WebSocket 连接
                try:
                    session.websocket = await self._connect_websocket()
                except Exception as e:
                    logger.error(f"Xiling TTS WebSocket 连接失败: {e}")
                    context.sessions.pop(input_stream_key, None)
                    return
                
                # 初始化会话 (发送 system.start 并等待 system.started)
                try:
                    init_ok = await self._initialize_session(session.websocket)
                except Exception as e:
                    logger.error(f"Xiling TTS 初始化失败: {e}")
                    init_ok = False

                if not init_ok:
                    logger.error("Xiling TTS: 初始化未成功，终止本次合成")
                    session.close_websocket()
                    context.sessions.pop(input_stream_key, None)
                    return
        
        text = data.data.get_main_data()
        text = self._filter_text(text)
        text_end = data.is_last_data
        
        if not text:
            if text_end and session:
                # 发送结束帧
                finish_msg = {"type": "system.finish"}
                try:
                    await session.websocket.send(json.dumps(finish_msg))
                except Exception as e:
                    logger.error(f"发送结束帧失败: {e}")
            return
        
        try:
            if not text_end:
                # 发送文本帧
                msg = {"type": "text", "payload": {"text": text}}
                await session.websocket.send(json.dumps(msg))
                
                # 开始接收音频数据
                await self._receive_audio(context, session, finish=False)
            else:
                # 最后一段文本
                msg = {"type": "text", "payload": {"text": text}}
                await session.websocket.send(json.dumps(msg))
                
                # 发送结束帧
                finish_msg = {"type": "system.finish"}
                await session.websocket.send(json.dumps(finish_msg))
                
                # 接收剩余音频
                await self._receive_audio(context, session, finish=True)
                
                # 缓存 WebSocket 连接以供下次复用（节省 ~250ms 连接时间）
                if session.websocket is not None:
                    try:
                        from websockets.protocol import State
                        ws_open = session.websocket.state == State.OPEN
                    except Exception:
                        ws_open = not getattr(session.websocket, 'closed', True)
                    if ws_open:
                        context._cached_websocket = session.websocket
                        session.websocket = None
                session.reset()
                context.sessions.pop(input_stream_key, None)
                
        except Exception as e:
            logger.error(f"Xiling TTS 处理失败: {e}")
            session.close_websocket()
            context.sessions.pop(input_stream_key, None)
    
    async def _receive_audio(self, context: XilingTTSContext, session: XilingTTSSession, finish: bool = False):
        """接收音频数据"""
        if not session.websocket:
            return
        
        streamer = context.data_submitter.get_streamer(ChatDataType.AVATAR_AUDIO)
        
        try:
            # 设置超时（根据配置）
            timeout = self.config.audio_finish_timeout if finish else self.config.audio_timeout
            
            while True:
                try:
                    msg = await asyncio.wait_for(
                        session.websocket.recv(),
                        timeout=timeout
                    )
                except asyncio.TimeoutError:
                    if finish:
                        break
                    return
                
                if isinstance(msg, bytes):
                    # 二进制音频数据
                    session.audio_buffer.extend(msg)
                    
                    # 累积到一定量后提交
                    bytes_per_second = self.config.sample_rate * 2  # 16bit = 2 bytes
                    chunk_size = bytes_per_second // 10  # 100ms
                    
                    while len(session.audio_buffer) >= chunk_size:
                        chunk = bytes(session.audio_buffer[:chunk_size])
                        session.audio_buffer = session.audio_buffer[chunk_size:]
                        
                        # 转换为 float32
                        audio_array = np.frombuffer(chunk, dtype=np.int16).astype(np.float32) / 32767
                        audio_array = audio_array[np.newaxis, ...]
                        
                        output = DataBundle(streamer.data_definition)
                        output.set_main_data(audio_array)
                        context.submit_data(output)
                        
                elif isinstance(msg, str):
                    # 文本控制消息
                    data = json.loads(msg)
                    msg_type = data.get("type", "")
                    
                    if msg_type == "system.started":
                        logger.info(f"Xiling TTS 开始合成: {data}")
                    elif msg_type == "system.finished":
                        logger.info("Xiling TTS 合成完成")
                        break
                    elif msg_type == "system.error":
                        logger.error(f"Xiling TTS 错误: {data}")
                        break
                        
        except websockets.exceptions.ConnectionClosed:
            logger.warning("Xiling TTS WebSocket 连接关闭")
        except Exception as e:
            logger.error(f"接收音频失败: {e}")
        
        # 提交剩余音频
        if len(session.audio_buffer) > 0 and finish:
            chunk = bytes(session.audio_buffer)
            audio_array = np.frombuffer(chunk, dtype=np.int16).astype(np.float32) / 32767
            audio_array = audio_array[np.newaxis, ...]
            
            output = DataBundle(streamer.data_definition)
            output.set_main_data(audio_array)
            context.submit_data(output)
            session.audio_buffer = bytearray()
        
        # 发送结束帧
        if finish:
            output = DataBundle(streamer.data_definition)
            output.set_main_data(np.zeros(shape=(1, 240), dtype=np.float32))
            context.submit_data(output, finish_stream=True)
    
    def handle(self, context: HandlerContext, inputs: ChatData,
               output_definitions: Dict[ChatDataType, HandlerDataInfo]):
        """处理输入数据"""
        context = cast(XilingTTSContext, context)

        if inputs.type == ChatDataType.AVATAR_TEXT:
            text_data = inputs.data.get_main_data() if inputs.data else None
            logger.info(f"Xiling TTS handle: 收到文本输入, text={str(text_data)[:50] if text_data else 'None'}")

            # 使用事件循环运行异步处理
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    asyncio.create_task(self._handle_text_stream_async(context, inputs))
                else:
                    loop.run_until_complete(self._handle_text_stream_async(context, inputs))
            except RuntimeError:
                # 没有事件循环，创建新线程运行
                def run_async():
                    asyncio.run(self._handle_text_stream_async(context, inputs))
                threading.Thread(target=run_async, daemon=True).start()
    
    def on_signal(self, context: HandlerContext, signal: ChatSignal):
        """处理打断信号"""
        context = cast(XilingTTSContext, context)
        
        if signal.type == ChatSignalType.STREAM_CANCEL and signal.related_stream:
            stream_key = signal.related_stream.key
            if stream_key is None:
                return
            
            # 检查输入流
            session = context.sessions.pop(stream_key, None)
            if session:
                logger.info(f"Xiling TTS: 取消会话 {stream_key}")
                session.close_websocket()
                return
            
            # 检查输出流
            for key, session in list(context.sessions.items()):
                if session.output_stream_key == stream_key:
                    logger.info(f"Xiling TTS: 取消输出流 {stream_key}")
                    session.close_websocket()
                    context.sessions.pop(key, None)
                    return
    
    def destroy_context(self, context: HandlerContext):
        """销毁上下文"""
        context = cast(XilingTTSContext, context)
        logger.info("Xiling TTS 销毁上下文")
        
        for session in context.sessions.values():
            try:
                session.close_websocket()
            except Exception as e:
                logger.warning(f"重置会话失败: {e}")
        
        # 关闭缓存的 WebSocket
        if context._cached_websocket is not None:
            try:
                loop = asyncio.new_event_loop()
                loop.run_until_complete(context._cached_websocket.close())
                loop.close()
            except Exception:
                pass
            context._cached_websocket = None
        
        context.sessions.clear()
