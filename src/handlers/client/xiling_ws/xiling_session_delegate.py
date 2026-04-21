"""
Xiling WebSocket Session Delegate
处理客户端输入和消息回显
"""
import asyncio
import json
import time
from typing import Dict, Optional, Union, Tuple, Any
from uuid import uuid4

import numpy as np
from loguru import logger

from chat_engine.common.client_handler_base import ClientSessionDelegate
from chat_engine.contexts.session_clock import SessionClock
from chat_engine.data_models.chat_data.chat_data_model import ChatData
from chat_engine.data_models.chat_data_type import ChatDataType
from chat_engine.data_models.chat_signal import ChatSignal
from chat_engine.data_models.engine_channel_type import EngineChannelType
from chat_engine.data_models.runtime_data.data_bundle import DataBundle, DataBundleDefinition, DataBundleEntry


class XilingSessionDelegate(ClientSessionDelegate):
    """
    Xiling WebSocket Session Delegate
    处理文本输入、音频输入和信号输出
    """

    def __init__(self, heartbeat_timeout: float = 30.0):
        self.session_id: Optional[str] = None
        self.clock: Optional[SessionClock] = None
        self.data_submitter = None
        self.shared_states = None
        self.signal_emitter = None
        self.stream_manager = None

        # 输出队列
        self.output_queues = {
            EngineChannelType.AUDIO: asyncio.Queue(),
            EngineChannelType.VIDEO: asyncio.Queue(),
            EngineChannelType.TEXT: asyncio.Queue(),
        }

        # 输入数据定义
        self.input_data_definitions: Dict[EngineChannelType, DataBundleDefinition] = {}

        # 模态映射
        self.modality_mapping = {
            EngineChannelType.AUDIO: ChatDataType.MIC_AUDIO,
            EngineChannelType.VIDEO: ChatDataType.CAMERA_VIDEO,
            EngineChannelType.TEXT: ChatDataType.HUMAN_TEXT,
        }

        # 会话状态
        self.quit = asyncio.Event()
        self.heartbeat_timeout = heartbeat_timeout
        self.last_heartbeat_time = time.time()

    def get_timestamp(self) -> Tuple[int, int]:
        """获取时间戳"""
        if self.clock:
            return self.clock.get_timestamp()
        return (0, 16000)

    def put_text_data(self, text: str, stream_id: str):
        """提交文本数据到引擎（作为 AVATAR_TEXT 直接驱动 TTS，无需 LLM）"""
        logger.info(f"XilingSessionDelegate.put_text_data: text={text[:50]}..., stream_id={stream_id}")
        if self.data_submitter is None:
            return

        definition = self.input_data_definitions.get(EngineChannelType.TEXT)
        if definition is None:
            logger.warning("XilingSessionDelegate: no TEXT definition, cannot submit text")
            return

        data_bundle = DataBundle(definition)
        data_bundle.set_main_data(text)

        chat_data = ChatData(
            source="xiling_ws",
            type=ChatDataType.AVATAR_TEXT,
            data=data_bundle,
            timestamp=self.get_timestamp(),
            is_last_data=True,
        )

        if stream_id:
            from chat_engine.data_models.chat_stream import ChatStreamIdentity
            chat_data.stream_id = ChatStreamIdentity(
                stream_key_str=stream_id,
                data_type=ChatDataType.AVATAR_TEXT,
                producer_name="xiling_ws"
            )

        self.data_submitter.submit(chat_data, finish_stream=True)

    def put_data(self, modality: EngineChannelType, data: Union[np.ndarray, str],
                 timestamp: Optional[Tuple[int, int]] = None,
                 samplerate: Optional[int] = None, loopback: bool = False):
        """从客户端接收数据并提交到引擎"""
        if timestamp is None:
            timestamp = self.get_timestamp()
        if self.data_submitter is None:
            return

        definition = self.input_data_definitions.get(modality)
        chat_data_type = self.modality_mapping.get(modality)
        if chat_data_type is None or definition is None:
            return

        data_bundle = DataBundle(definition)
        is_last_data = False

        if modality == EngineChannelType.AUDIO:
            data_bundle.set_main_data(data.squeeze()[np.newaxis, ...])
        elif modality == EngineChannelType.VIDEO:
            data_bundle.set_main_data(data[np.newaxis, ...])
        elif modality == EngineChannelType.TEXT:
            data_bundle.set_main_data(data)
            is_last_data = True
        else:
            return

        chat_data = ChatData(
            source="xiling_ws",
            type=chat_data_type,
            data=data_bundle,
            timestamp=timestamp,
        )
        self.data_submitter.submit(chat_data, finish_stream=is_last_data)

        if loopback:
            self.output_queues[modality].put_nowait(chat_data)

    async def get_data(self, modality: EngineChannelType, timeout: Optional[float] = 0.1) -> Optional[ChatData]:
        """从引擎获取处理后的数据"""
        data_queue = self.output_queues.get(modality)
        if data_queue is None:
            return None

        if timeout is not None and timeout > 0:
            try:
                return await asyncio.wait_for(data_queue.get(), timeout)
            except asyncio.TimeoutError:
                return None
        else:
            return await data_queue.get()

    def emit_signal(self, signal: ChatSignal):
        """发送信号到引擎"""
        if self.signal_emitter:
            self.signal_emitter.emit(signal)

    def clear_data(self):
        """清空数据队列"""
        for queue in self.output_queues.values():
            while not queue.empty():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

    def submit_text(self, text: str, stream_id: Optional[str] = None) -> ChatData:
        """
        提交文本到引擎

        Args:
            text: 要提交的文本
            stream_id: 可选的流ID

        Returns:
            创建的 ChatData
        """
        definition = DataBundleDefinition()
        definition.add_entry(DataBundleEntry.create_text_entry("human_text"))
        definition.lockdown()

        bundle = DataBundle(definition)
        bundle.set_main_data(text)

        chat_data = ChatData(
            source="xiling_ws",
            type=ChatDataType.HUMAN_TEXT,
            data=bundle,
            is_last_data=True,
        )

        if stream_id:
            from chat_engine.data_models.chat_stream import ChatStreamIdentity
            chat_data.stream_id = ChatStreamIdentity(
                stream_key_str=stream_id,
                data_type=ChatDataType.HUMAN_TEXT,
                producer_name="xiling_ws"
            )

        if self.data_submitter:
            self.data_submitter.submit(chat_data, finish_stream=True)

        return chat_data

    def submit_audio(self, audio_data: np.ndarray, sample_rate: int = 16000,
                     is_last: bool = False, stream_id: Optional[str] = None) -> ChatData:
        """
        提交音频到引擎（直接驱动数字人）

        Args:
            audio_data: 音频数据 (int16 数组)
            sample_rate: 采样率
            is_last: 是否是最后一段音频
            stream_id: 可选的流ID

        Returns:
            创建的 ChatData
        """
        definition = DataBundleDefinition()
        definition.add_entry(DataBundleEntry.create_audio_entry(
            "avatar_audio",
            1,
            sample_rate
        ))
        definition.lockdown()

        bundle = DataBundle(definition)
        bundle.set_main_data(audio_data[np.newaxis, ...])

        chat_data = ChatData(
            source="xiling_ws",
            type=ChatDataType.AVATAR_AUDIO,
            data=bundle,
            is_last_data=is_last,
        )

        if stream_id:
            from chat_engine.data_models.chat_stream import ChatStreamIdentity
            chat_data.stream_id = ChatStreamIdentity(
                stream_key_str=stream_id,
                data_type=ChatDataType.AVATAR_AUDIO,
                producer_name="xiling_ws"
            )

        if self.data_submitter:
            self.data_submitter.submit(chat_data, finish_stream=is_last)

        return chat_data
