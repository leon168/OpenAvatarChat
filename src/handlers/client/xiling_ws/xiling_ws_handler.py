"""
Xiling WebSocket Handler
与百度希灵数字人平台 WebSocket API 兼容的处理器

WebSocket 地址格式:
ws://host:port/ws/xiling?appId=${app_id}&token=${token}&liveRoom=${live_room}

支持的消息类型:
- TEXT: 文本驱动数字人说话
- START/COMPLETE: 音频流驱动数字人
- INTERRUPT: 打断当前播报
"""

import asyncio
import json
import time
import uuid
from typing import Optional, Dict, Any, Set
from dataclasses import dataclass, field

import numpy as np
from fastapi import WebSocket, WebSocketDisconnect
from loguru import logger
from pydantic import BaseModel, Field
from starlette.websockets import WebSocketState

from chat_engine.common.client_handler_base import ClientHandlerBase, ClientSessionDelegate
from chat_engine.common.handler_base import HandlerDataInfo, HandlerDetail, HandlerBaseInfo
from chat_engine.contexts.handler_context import HandlerContext
from chat_engine.contexts.session_context import SessionContext
from chat_engine.data_models.chat_data.chat_data_model import ChatData
from chat_engine.data_models.chat_data_type import ChatDataType
from chat_engine.data_models.chat_engine_config_data import HandlerBaseConfigModel, ChatEngineConfigModel
from chat_engine.data_models.chat_signal import ChatSignal
from chat_engine.data_models.chat_signal_type import ChatSignalType, ChatSignalSourceType
from chat_engine.data_models.chat_stream_config import ChatStreamConfig
from chat_engine.data_models.engine_channel_type import EngineChannelType
from chat_engine.data_models.runtime_data.data_bundle import DataBundleDefinition, DataBundleEntry, VariableSize

from .xiling_message_protocol import (
    XilingMessage, MessageType,
    TextMessageBody, StartMessageBody, CompleteMessageBody,
    ErrorMessageBody, ReadyMessageBody,
    InterruptMessageBody, InterruptedMessageBody
)
from .xiling_session_delegate import XilingSessionDelegate


class XilingWsConfig(HandlerBaseConfigModel, BaseModel):
    """希灵 WebSocket 配置"""
    path: str = Field(default="/ws/xiling", description="WebSocket 路径")
    sample_rate: int = Field(default=16000, description="音频采样率")
    connection_ttl: int = Field(default=3600, description="连接最长存活时间(秒)")
    heartbeat_interval: float = Field(default=30.0, description="心跳检测间隔(秒)")
    enable_ping_pong: bool = Field(default=True, description="启用 Ping/Pong 机制")
    persist_session: bool = Field(default=True, description="客户端断连后是否保持 Session（直播场景需要）")


@dataclass
class XilingConnection:
    """希灵 WebSocket 连接上下文"""
    connection_id: str
    websocket: WebSocket
    app_id: str
    token: str
    live_room: str
    session_delegate: Optional[XilingSessionDelegate] = None
    created_at: float = field(default_factory=time.time)
    last_ping_time: float = field(default_factory=time.time)
    current_stream_id: Optional[str] = None
    audio_buffer: bytearray = field(default_factory=bytearray)
    is_audio_streaming: bool = False
    quit: asyncio.Event = field(default_factory=asyncio.Event)


class XilingWsHandler(ClientHandlerBase):
    """
    希灵 WebSocket Handler
    
    提供与百度希灵数字人平台兼容的 WebSocket API:
    - 支持文本驱动 (TEXT)
    - 支持音频流驱动 (START/COMPLETE)
    - 支持打断 (INTERRUPT)
    - 支持 Ping/Pong 心跳
    """
    
    def __init__(self):
        super().__init__()
        self.config: Optional[XilingWsConfig] = None
        self.active_connections: Dict[str, XilingConnection] = {}
        self._cleanup_task: Optional[asyncio.Task] = None
        
        # 数据定义
        self.input_bundle_definitions: Dict[EngineChannelType, DataBundleDefinition] = {}
        self.output_bundle_definitions: Dict[EngineChannelType, DataBundleDefinition] = {}
    
    def get_handler_info(self) -> HandlerBaseInfo:
        """获取 Handler 信息"""
        return HandlerBaseInfo(
            config_model=XilingWsConfig,
            client_session_delegate_class=XilingSessionDelegate,
        )
    
    def load(self, engine_config: ChatEngineConfigModel, handler_config: Optional[XilingWsConfig] = None):
        """加载 Handler"""
        self.config = handler_config or XilingWsConfig()
        self._prepare_data_definitions()
        logger.info(f"XilingWsHandler loaded with path={self.config.path}")
    
    def _prepare_data_definitions(self):
        """准备数据定义"""
        # 音频输入定义 (从客户端接收)
        audio_input_definition = DataBundleDefinition()
        audio_input_definition.add_entry(DataBundleEntry.create_audio_entry(
            "mic_audio",
            1,  # mono
            self.config.sample_rate,
        ))
        audio_input_definition.lockdown()
        self.input_bundle_definitions[EngineChannelType.AUDIO] = audio_input_definition

        # 音频输出定义 (发送到客户端)
        audio_output_definition = DataBundleDefinition()
        audio_output_definition.add_entry(DataBundleEntry.create_audio_entry(
            "avatar_audio",
            1,
            24000,  # TTS 输出采样率
        ))
        audio_output_definition.lockdown()
        self.output_bundle_definitions[EngineChannelType.AUDIO] = audio_output_definition

        # 视频输出定义
        video_output_definition = DataBundleDefinition()
        video_output_definition.add_entry(DataBundleEntry.create_framed_entry(
            "avatar_video",
            [VariableSize(), VariableSize(), VariableSize(), 3],
            0,
            25
        ))
        video_output_definition.lockdown()
        self.output_bundle_definitions[EngineChannelType.VIDEO] = video_output_definition

        # 文本输出定义 (text drive: 客户端文本直接驱动 TTS)
        text_output_definition = DataBundleDefinition()
        text_output_definition.add_entry(DataBundleEntry.create_text_entry("avatar_text"))
        text_output_definition.lockdown()
        self.output_bundle_definitions[EngineChannelType.TEXT] = text_output_definition
    
    def on_setup_app(self, app, ui, parent_block):
        """注册 WebSocket 路由"""

        @app.websocket(self.config.path)
        async def xiling_ws_endpoint(websocket: WebSocket):
            await self._handle_connection(websocket)

        # 使用 FastAPI 启动事件来启动清理任务
        @app.on_event("startup")
        async def startup_event():
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())
            logger.info(f"Xiling cleanup task started")

        @app.on_event("shutdown")
        async def shutdown_event():
            if self._cleanup_task and not self._cleanup_task.done():
                self._cleanup_task.cancel()
                logger.info(f"Xiling cleanup task cancelled")

        logger.info(f"Xiling WebSocket endpoint registered at {self.config.path}")
    
    async def _cleanup_loop(self):
        """定期清理过期连接"""
        while True:
            try:
                await asyncio.sleep(60)  # 每分钟检查一次
                current_time = time.time()
                expired_connections = []
                
                for conn_id, conn in self.active_connections.items():
                    # 检查连接是否过期
                    if current_time - conn.created_at > self.config.connection_ttl:
                        expired_connections.append(conn_id)
                        continue
                    
                    # 检查心跳超时
                    if self.config.enable_ping_pong:
                        if current_time - conn.last_ping_time > self.config.heartbeat_interval * 2:
                            logger.warning(f"Connection {conn_id} heartbeat timeout")
                            expired_connections.append(conn_id)
                
                # 关闭过期连接
                for conn_id in expired_connections:
                    await self._close_connection(conn_id)
                    
            except Exception as e:
                logger.error(f"Cleanup loop error: {e}")
    
    async def _handle_connection(self, websocket: WebSocket):
        """处理 WebSocket 连接"""
        # 解析查询参数
        params = dict(websocket.query_params)
        app_id = params.get("appId", "")
        token = params.get("token", "")
        live_room = params.get("liveRoom", str(uuid.uuid4()))
        
        # 生成连接ID
        connection_id = f"{live_room}_{uuid.uuid4().hex[:8]}"
        
        logger.info(f"Xiling WS 连接: connection_id={connection_id}, appId={app_id}, liveRoom={live_room}")
        
        await websocket.accept()
        
        # 创建连接上下文
        connection = XilingConnection(
            connection_id=connection_id,
            websocket=websocket,
            app_id=app_id,
            token=token,
            live_room=live_room,
        )
        self.active_connections[connection_id] = connection
        
        try:
            # 创建会话
            session_delegate = await self._create_session(connection)
            if session_delegate is None:
                await self._send_error(connection, 0, "Failed to create session")
                return
            
            connection.session_delegate = session_delegate
            
            # 发送 READY 消息
            await self._send_ready(connection)
            
            # 启动消息处理循环
            await self._message_loop(connection)
            
        except WebSocketDisconnect:
            logger.info(f"Connection {connection_id} disconnected")
        except Exception as e:
            logger.error(f"Connection {connection_id} error: {e}")
            await self._send_error(connection, 0, f"Internal error: {str(e)}")
        finally:
            await self._close_connection(connection_id)
    
    async def _create_session(self, connection: XilingConnection) -> Optional[XilingSessionDelegate]:
        """创建或复用会话"""
        try:
            # 先尝试复用已有 session（persist_session 场景下重连）
            existing = self.handler_delegate.find_session_delegate(connection.live_room)
            if existing is not None:
                logger.info(f"Reusing existing session: {connection.live_room}")
                return existing

            session_delegate = self.handler_delegate.start_session(
                session_id=connection.live_room,
                user_id=connection.app_id,
                timestamp_base=self.config.sample_rate,
            )
            
            # 设置数据定义
            session_delegate.input_data_definitions = self.input_bundle_definitions
            
            return session_delegate
        except Exception as e:
            logger.error(f"Failed to create session: {e}")
            return None
    
    async def _message_loop(self, connection: XilingConnection):
        """消息处理循环"""
        connection.quit.clear()
        while not connection.quit.is_set():
            try:
                # 接收消息
                message = await connection.websocket.receive()

                msg_type = message.get("type", "")

                # 处理断开连接消息
                if msg_type == "websocket.disconnect":
                    logger.info(f"Connection {connection.connection_id} received disconnect message")
                    connection.quit.set()
                    break

                if "text" in message:
                    await self._handle_text_message(connection, message["text"])
                elif "bytes" in message:
                    await self._handle_binary_message(connection, message["bytes"])
                elif msg_type == "websocket.ping":
                    # 处理 Ping 帧
                    await self._handle_ping(connection, message.get("bytes", b""))

            except WebSocketDisconnect:
                logger.info(f"Connection {connection.connection_id} disconnected by client")
                connection.quit.set()
            except Exception as e:
                logger.error(f"Message loop error: {e}")
                connection.quit.set()
    
    async def _handle_text_message(self, connection: XilingConnection, text: str):
        """处理文本消息"""
        try:
            data = json.loads(text)
            message = XilingMessage.from_dict(data)
            
            logger.debug(f"Received message: id={message.id}, type={message.type}")
            
            # 根据消息类型分发处理
            handlers = {
                MessageType.TEXT.value: self._handle_text_drive,
                MessageType.START.value: self._handle_audio_start,
                MessageType.COMPLETE.value: self._handle_audio_complete,
                MessageType.INTERRUPT.value: self._handle_interrupt,
            }
            
            handler = handlers.get(message.type)
            if handler:
                await handler(connection, message)
            else:
                await self._send_error(connection, message.id, f"Unknown message type: {message.type}")
                
        except json.JSONDecodeError as e:
            await self._send_error(connection, 0, f"Invalid JSON: {e}")
        except Exception as e:
            logger.error(f"Handle text message error: {e}")
            await self._send_error(connection, 0, f"Internal error: {e}")
    
    async def _handle_binary_message(self, connection: XilingConnection, data: bytes):
        """处理二进制消息 (音频数据)"""
        if not connection.is_audio_streaming:
            logger.warning("Received binary data but audio streaming not started")
            return
        
        try:
            # 累积音频数据
            connection.audio_buffer.extend(data)
            
            # 转换为 numpy 数组
            audio_array = np.frombuffer(data, dtype=np.int16)
            
            # 提交到会话
            if connection.session_delegate:
                connection.session_delegate.submit_audio(audio_array, self.config.sample_rate)
                
        except Exception as e:
            logger.error(f"Handle binary message error: {e}")
    
    async def _handle_text_drive(self, connection: XilingConnection, message: XilingMessage):
        """处理 TEXT 消息 - 文本驱动"""
        try:
            body = TextMessageBody.from_json(message.body)

            if not body.text:
                await self._send_error(connection, message.id, "Text is empty")
                return

            logger.info(f"Text drive: streamId={body.streamId}, text={body.text[:50]}...")

            # 发送 START 确认
            logger.info(f"Sending START response for stream {body.streamId}")
            await self._send_message(
                connection, message.id, MessageType.START.value,
                StartMessageBody(streamId=body.streamId, event="START")
            )

            # 提交文本到会话
            if connection.session_delegate:
                logger.info(f"Submitting text to session delegate")
                connection.session_delegate.put_text_data(body.text, body.streamId)
            else:
                logger.warning(f"Session delegate is None for connection {connection.connection_id}")

            # 发送 COMPLETE
            logger.info(f"Sending COMPLETE response for stream {body.streamId}")
            await self._send_message(
                connection, message.id, MessageType.COMPLETE.value,
                CompleteMessageBody(streamId=body.streamId, event="COMPLETE")
            )

        except Exception as e:
            logger.error(f"Handle text drive error: {e}")
            import traceback
            traceback.print_exc()
            await self._send_error(connection, message.id, f"Text drive failed: {e}")
    
    async def _handle_audio_start(self, connection: XilingConnection, message: XilingMessage):
        """处理 START 消息 - 开始音频流"""
        try:
            body = StartMessageBody.from_json(message.body)
            
            connection.current_stream_id = body.streamId
            connection.is_audio_streaming = True
            connection.audio_buffer = bytearray()
            
            logger.info(f"Audio streaming started: streamId={body.streamId}")
            
            # 发送 READY 确认
            await self._send_message(
                connection, message.id, MessageType.START.value,
                StartMessageBody(streamId=body.streamId, event="READY")
            )
            
        except Exception as e:
            logger.error(f"Handle audio start error: {e}")
            await self._send_error(connection, message.id, f"Start failed: {e}")
    
    async def _handle_audio_complete(self, connection: XilingConnection, message: XilingMessage):
        """处理 COMPLETE 消息 - 音频流完成"""
        try:
            body = CompleteMessageBody.from_json(message.body)
            
            logger.info(f"Audio streaming completed: streamId={body.streamId}")
            
            # 处理剩余音频数据
            if len(connection.audio_buffer) > 0 and connection.session_delegate:
                audio_array = np.frombuffer(connection.audio_buffer, dtype=np.int16)
                connection.session_delegate.submit_audio(
                    audio_array, self.config.sample_rate, is_last=True
                )
            
            # 重置状态
            connection.is_audio_streaming = False
            connection.audio_buffer = bytearray()
            
            # 发送 COMPLETE 确认
            await self._send_message(
                connection, message.id, MessageType.COMPLETE.value,
                CompleteMessageBody(streamId=body.streamId, event="COMPLETE")
            )
            
        except Exception as e:
            logger.error(f"Handle audio complete error: {e}")
            await self._send_error(connection, message.id, f"Complete failed: {e}")
    
    async def _handle_interrupt(self, connection: XilingConnection, message: XilingMessage):
        """处理 INTERRUPT 消息 - 打断"""
        try:
            body = InterruptMessageBody.from_json(message.body) if message.body else None
            stream_id = body.streamId if body else connection.current_stream_id
            
            logger.info(f"Interrupt: streamId={stream_id}")
            
            # 发送打断信号
            if connection.session_delegate:
                signal = ChatSignal(
                    type=ChatSignalType.INTERRUPT,
                    source_type=ChatSignalSourceType.CLIENT,
                    source_name="xiling_ws"
                )
                connection.session_delegate.emit_signal(signal)
            
            # 重置音频流状态
            connection.is_audio_streaming = False
            connection.audio_buffer = bytearray()
            connection.current_stream_id = None
            
            # 发送 INTERRUPTED 确认
            await self._send_message(
                connection, message.id, MessageType.INTERRUPTED.value,
                InterruptedMessageBody(streamId=stream_id, event="INTERRUPTED")
            )
            
        except Exception as e:
            logger.error(f"Handle interrupt error: {e}")
            await self._send_error(connection, message.id, f"Interrupt failed: {e}")
    
    async def _handle_ping(self, connection: XilingConnection, payload: bytes):
        """处理 Ping 消息"""
        connection.last_ping_time = time.time()
        # 发送 Pong 响应
        await connection.websocket.send_bytes(payload)
    
    async def _send_ready(self, connection: XilingConnection):
        """发送 READY 消息"""
        await self._send_message(
            connection, 0, MessageType.READY.value,
            ReadyMessageBody(streamId=connection.live_room, event="READY")
        )
    
    async def _send_error(self, connection: XilingConnection, msg_id: int, error: str):
        """发送错误消息"""
        await self._send_message(
            connection, msg_id, MessageType.ERROR.value,
            ErrorMessageBody(event="ERROR", message=error)
        )
    
    async def _send_message(self, connection: XilingConnection, msg_id: int, msg_type: str, body: Any):
        """发送消息"""
        try:
            message = XilingMessage(
                id=msg_id,
                type=msg_type,
                body=body.to_json()
            )
            await connection.websocket.send_json(message.to_dict())
        except Exception as e:
            logger.error(f"Send message error: {e}")
    
    async def _close_connection(self, connection_id: str):
        """关闭连接"""
        connection = self.active_connections.pop(connection_id, None)
        if connection is None:
            return

        logger.info(f"Closing connection: {connection_id}")

        # 停止会话
        if connection.session_delegate:
            try:
                if self.config.persist_session:
                    # 直播场景：客户端断连不销毁 session，让数据流继续完成
                    logger.info(f"Session {connection.live_room} persisting after client disconnect")
                else:
                    self.handler_delegate.stop_session(connection.live_room)
            except Exception as e:
                logger.error(f"Stop session error: {e}")

        # 关闭 WebSocket
        try:
            connection.quit.set()
            if connection.websocket.client_state != WebSocketState.DISCONNECTED:
                await connection.websocket.close()
        except Exception:
            pass
    
    def on_setup_session_delegate(self, session_context: SessionContext, handler_context: HandlerContext,
                                  session_delegate: ClientSessionDelegate):
        """设置会话委托"""
        if isinstance(session_delegate, XilingSessionDelegate):
            session_delegate.session_id = session_context.session_info.session_id
            session_delegate.clock = session_context.get_clock()
            session_delegate.data_submitter = handler_context.data_submitter
            session_delegate.signal_emitter = handler_context.signal_emitter
            session_delegate.input_data_definitions = self.input_bundle_definitions
            session_delegate.shared_states = session_context.shared_states
    
    def create_context(self, session_context: SessionContext,
                       handler_config: Optional[XilingWsConfig] = None) -> HandlerContext:
        """创建 Handler Context"""
        context = HandlerContext(session_context.session_info.session_id)
        return context
    
    def start_context(self, session_context: SessionContext, handler_context: HandlerContext):
        """启动 Context"""
        pass
    
    def get_handler_detail(self, session_context: SessionContext, context: HandlerContext) -> HandlerDetail:
        """获取 Handler Detail"""
        # 移除对 AVATAR_AUDIO/VIDEO 的订阅，避免与 SRTOutput 冲突
        # 状态回报通过 WebSocket 消息协议实现（START/COMPLETE/INTERRUPTED 等）
        # 参考百度希灵协议：https://xiling.cloud.baidu.com/doc/AI_DH/s/Sm1h9a4dh
        inputs = {}

        _no_link = ChatStreamConfig(cancelable=False, auto_link_input=False)
        _tts_text = ChatStreamConfig(cancelable=True)
        outputs = {
            ChatDataType.MIC_AUDIO: HandlerDataInfo(
                type=ChatDataType.MIC_AUDIO,
                definition=self.output_bundle_definitions.get(EngineChannelType.AUDIO),
                output_stream_config=_no_link,
            ),
            ChatDataType.AVATAR_TEXT: HandlerDataInfo(
                type=ChatDataType.AVATAR_TEXT,
                definition=self.output_bundle_definitions.get(EngineChannelType.TEXT),
                output_stream_config=_tts_text,
            ),
        }

        return HandlerDetail(inputs=inputs, outputs=outputs)
    
    def handle(self, context: HandlerContext, inputs: ChatData,
               output_definitions: Dict[ChatDataType, HandlerDataInfo]):
        """处理数据"""
        pass
    
    def destroy_context(self, context: HandlerContext):
        """销毁 Context"""
        pass
    
    def destroy(self):
        """销毁 Handler"""
        if self._cleanup_task:
            self._cleanup_task.cancel()
