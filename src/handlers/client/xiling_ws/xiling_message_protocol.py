"""
Xiling WebSocket Message Protocol
与百度希灵数字人平台 WebSocket API 兼容的消息协议定义

协议格式:
- 上行: {"id": int, "type": str, "body": json_str}
- 下行: {"id": int, "type": str, "body": json_str}
"""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any, Literal
from enum import Enum
import json


class MessageType(Enum):
    """消息类型枚举"""
    # 上行消息
    TEXT = "TEXT"           # 文本驱动
    START = "START"         # 开始音频流
    COMPLETE = "COMPLETE"   # 音频流完成
    INTERRUPT = "INTERRUPT" # 打断
    
    # 下行消息
    READY = "READY"         # 就绪
    ERROR = "ERROR"         # 错误
    INTERRUPTED = "INTERRUPTED"  # 已打断


@dataclass
class XilingMessage:
    """希灵协议消息基类"""
    id: int                 # 消息ID，大于0的整数，递增不能重复
    type: str               # 消息类型
    body: str               # 消息体，JSON字符串
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "type": self.type,
            "body": self.body
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "XilingMessage":
        return cls(
            id=data.get("id", 0),
            type=data.get("type", ""),
            body=data.get("body", "{}")
        )
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_json(cls, json_str: str) -> "XilingMessage":
        return cls.from_dict(json.loads(json_str))


# ============================================================================
# 上行消息 Body 定义
# ============================================================================

@dataclass
class TextMessageBody:
    """TEXT 消息体 - 文本驱动数字人"""
    text: str               # 要播报的文本
    streamId: str           # 自定义流ID

    def to_json(self) -> str:
        return json.dumps({
            "text": self.text,
            "streamId": self.streamId
        })

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TextMessageBody":
        return cls(
            text=data.get("text", ""),
            streamId=data.get("streamId", "")
        )


@dataclass
class StartMessageBody:
    """START 消息体 - 开始音频流驱动"""
    streamId: str           # 流ID
    event: Literal["START"] = "START"
    sampleRate: int = 16000  # 采样率，默认16k

    def to_json(self) -> str:
        return json.dumps({
            "streamId": self.streamId,
            "event": self.event,
            "sampleRate": self.sampleRate
        })

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StartMessageBody":
        return cls(
            streamId=data.get("streamId", ""),
            event=data.get("event", "START"),
            sampleRate=data.get("sampleRate", 16000)
        )


@dataclass
class CompleteMessageBody:
    """COMPLETE 消息体 - 音频流发送完成"""
    streamId: str           # 流ID
    event: Literal["COMPLETE"] = "COMPLETE"

    def to_json(self) -> str:
        return json.dumps({
            "streamId": self.streamId,
            "event": self.event
        })

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CompleteMessageBody":
        return cls(
            streamId=data.get("streamId", ""),
            event=data.get("event", "COMPLETE")
        )


@dataclass
class InterruptMessageBody:
    """INTERRUPT 消息体 - 打断播报"""
    streamId: Optional[str] = None  # 可选，要打断的流ID

    def to_json(self) -> str:
        data = {}
        if self.streamId:
            data["streamId"] = self.streamId
        return json.dumps(data)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "InterruptMessageBody":
        return cls(streamId=data.get("streamId"))


# ============================================================================
# 下行消息 Body 定义
# ============================================================================

@dataclass
class ReadyMessageBody:
    """READY 消息体 - 服务端就绪"""
    streamId: str           # 会话/流ID
    event: Literal["READY"] = "READY"

    def to_json(self) -> str:
        return json.dumps({
            "streamId": self.streamId,
            "event": self.event
        })


@dataclass
class StartAckBody:
    """START 确认消息体"""
    streamId: str
    event: Literal["START"] = "START"

    def to_json(self) -> str:
        return json.dumps({
            "streamId": self.streamId,
            "event": self.event
        })


@dataclass
class CompleteAckBody:
    """COMPLETE 确认消息体"""
    streamId: str
    event: Literal["COMPLETE"] = "COMPLETE"

    def to_json(self) -> str:
        return json.dumps({
            "streamId": self.streamId,
            "event": self.event
        })


@dataclass
class InterruptedMessageBody:
    """INTERRUPTED 消息体 - 已打断"""
    streamId: Optional[str]
    event: Literal["INTERRUPTED"] = "INTERRUPTED"

    def to_json(self) -> str:
        data = {"event": self.event}
        if self.streamId:
            data["streamId"] = self.streamId
        return json.dumps(data)


@dataclass
class ErrorMessageBody:
    """ERROR 消息体"""
    event: Literal["ERROR"] = "ERROR"
    message: str = ""         # 错误信息
    code: Optional[int] = None  # 错误码

    def to_json(self) -> str:
        data = {
            "event": self.event,
            "message": self.message
        }
        if self.code is not None:
            data["code"] = self.code
        return json.dumps(data)


# ============================================================================
# 消息构造辅助函数
# ============================================================================

def create_text_message(msg_id: int, text: str, stream_id: str) -> XilingMessage:
    """创建文本驱动消息"""
    body = TextMessageBody(text=text, streamId=stream_id)
    return XilingMessage(id=msg_id, type=MessageType.TEXT.value, body=body.to_json())


def create_start_message(msg_id: int, stream_id: str, sample_rate: int = 16000) -> XilingMessage:
    """创建开始音频流消息"""
    body = StartMessageBody(streamId=stream_id, sampleRate=sample_rate)
    return XilingMessage(id=msg_id, type=MessageType.START.value, body=body.to_json())


def create_complete_message(msg_id: int, stream_id: str) -> XilingMessage:
    """创建音频流完成消息"""
    body = CompleteMessageBody(streamId=stream_id)
    return XilingMessage(id=msg_id, type=MessageType.COMPLETE.value, body=body.to_json())


def create_interrupt_message(msg_id: int, stream_id: Optional[str] = None) -> XilingMessage:
    """创建打断消息"""
    body = InterruptMessageBody(streamId=stream_id)
    return XilingMessage(id=msg_id, type=MessageType.INTERRUPT.value, body=body.to_json())


def create_ready_message(msg_id: int, stream_id: str) -> XilingMessage:
    """创建就绪消息（下行）"""
    body = ReadyMessageBody(streamId=stream_id)
    return XilingMessage(id=msg_id, type=MessageType.READY.value, body=body.to_json())


def create_start_ack_message(msg_id: int, stream_id: str) -> XilingMessage:
    """创建开始确认消息（下行）"""
    body = StartAckBody(streamId=stream_id)
    return XilingMessage(id=msg_id, type=MessageType.START.value, body=body.to_json())


def create_complete_ack_message(msg_id: int, stream_id: str) -> XilingMessage:
    """创建完成确认消息（下行）"""
    body = CompleteAckBody(streamId=stream_id)
    return XilingMessage(id=msg_id, type=MessageType.COMPLETE.value, body=body.to_json())


def create_interrupted_message(msg_id: int, stream_id: Optional[str] = None) -> XilingMessage:
    """创建已打断消息（下行）"""
    body = InterruptedMessageBody(streamId=stream_id)
    return XilingMessage(id=msg_id, type=MessageType.INTERRUPTED.value, body=body.to_json())


def create_error_message(msg_id: int, message: str, code: Optional[int] = None) -> XilingMessage:
    """创建错误消息（下行）"""
    body = ErrorMessageBody(message=message, code=code)
    return XilingMessage(id=msg_id, type=MessageType.ERROR.value, body=body.to_json())
