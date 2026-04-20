"""
Mock 测试：Xiling WebSocket Handler 消息解析测试

不依赖完整项目，只测试消息格式解析逻辑
"""

import json
import asyncio
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable
from enum import Enum


class ChatDataType(Enum):
    """模拟 ChatDataType"""
    TEXT = "text"
    AVATAR_TEXT = "avatar_text"
    AVATAR_AUDIO = "avatar_audio"
    INTERRUPT = "interrupt"


@dataclass
class MockChatData:
    """模拟 ChatData"""
    type: ChatDataType
    data: str
    is_end: bool = False
    metadata: Dict = field(default_factory=dict)


class XilingMessageParser:
    """
    简化版消息解析器 - 从 xiling_ws_handler.py 提取的核心逻辑
    """
    
    def __init__(self):
        self.session_id: Optional[str] = None
        self.text_buffer: List[str] = []
        self.on_text_received: Optional[Callable[[str], None]] = None
        
    def parse_message(self, raw_data: str) -> Optional[MockChatData]:
        """
        解析 WebSocket 消息
        
        Args:
            raw_data: JSON 字符串
            
        Returns:
            MockChatData 或 None
        """
        try:
            data = json.loads(raw_data)
            msg_type = data.get("type", "")
            
            # 处理不同类型的消息
            if msg_type == "text":
                return self._handle_text_message(data)
            elif msg_type == "start":
                return self._handle_start_message(data)
            elif msg_type == "complete":
                return self._handle_complete_message(data)
            elif msg_type == "interrupt":
                return self._handle_interrupt_message(data)
            else:
                print(f"[警告] 未知消息类型: {msg_type}")
                return None
                
        except json.JSONDecodeError as e:
            print(f"[错误] JSON 解析失败: {e}")
            return None
        except Exception as e:
            print(f"[错误] 消息处理失败: {e}")
            return None
    
    def _handle_text_message(self, data: Dict) -> Optional[MockChatData]:
        """处理文本消息"""
        payload = data.get("payload", {})
        text = payload.get("text", "")
        
        if not text:
            print("[警告] 文本消息为空")
            return None
            
        self.text_buffer.append(text)
        
        if self.on_text_received:
            self.on_text_received(text)
            
        return MockChatData(
            type=ChatDataType.TEXT,
            data=text,
            is_end=False,
            metadata={"buffered": len(self.text_buffer)}
        )
    
    def _handle_start_message(self, data: Dict) -> Optional[MockChatData]:
        """处理开始消息"""
        payload = data.get("payload", {})
        self.session_id = payload.get("session_id")
        print(f"[开始] 新会话: {self.session_id}")
        
        return MockChatData(
            type=ChatDataType.TEXT,
            data="",
            is_end=False,
            metadata={"session_id": self.session_id, "event": "start"}
        )
    
    def _handle_complete_message(self, data: Dict) -> Optional[MockChatData]:
        """处理完成消息"""
        full_text = "".join(self.text_buffer)
        print(f"[完成] 完整文本 ({len(full_text)} 字符): {full_text[:50]}...")
        
        result = MockChatData(
            type=ChatDataType.AVATAR_TEXT,
            data=full_text,
            is_end=True,
            metadata={"total_chars": len(full_text)}
        )
        
        # 清空缓冲区
        self.text_buffer.clear()
        
        return result
    
    def _handle_interrupt_message(self, data: Dict) -> Optional[MockChatData]:
        """处理打断消息"""
        print("[打断] 收到打断信号，清空缓冲区")
        
        interrupted_text = "".join(self.text_buffer)
        self.text_buffer.clear()
        
        return MockChatData(
            type=ChatDataType.INTERRUPT,
            data=interrupted_text,
            is_end=True,
            metadata={"interrupted": True}
        )


class MockWebSocketServer:
    """
    模拟 WebSocket 服务端
    用于测试消息处理逻辑
    """
    
    def __init__(self):
        self.parser = XilingMessageParser()
        self.connected = False
        
    async def handle_client_message(self, message: str) -> List[MockChatData]:
        """
        处理客户端消息
        
        Args:
            message: WebSocket 消息
            
        Returns:
            处理结果列表
        """
        print(f"\n[接收] {message}")
        
        result = self.parser.parse_message(message)
        return [result] if result else []
    
    def simulate_conversation(self, messages: List[str]):
        """
        模拟对话流程
        
        Args:
            messages: 消息列表
        """
        print("=" * 60)
        print("模拟对话流程")
        print("=" * 60)
        
        for i, msg in enumerate(messages, 1):
            print(f"\n--- 消息 {i}/{len(messages)} ---")
            results = asyncio.run(self.handle_client_message(msg))
            
            for result in results:
                print(f"[输出] 类型={result.type.value}, 数据长度={len(result.data)}, 是否结束={result.is_end}")
        
        print("\n" + "=" * 60)
        print("对话结束")
        print("=" * 60)


def test_single_messages():
    """测试单个消息类型"""
    print("\n" + "=" * 60)
    print("测试 1: 单个消息解析")
    print("=" * 60)
    
    parser = XilingMessageParser()
    parser.on_text_received = lambda text: print(f"  [回调] 收到文本: {text}")
    
    # 测试文本消息
    text_msg = json.dumps({
        "type": "text",
        "payload": {"text": "你好，世界！"}
    })
    result = parser.parse_message(text_msg)
    assert result is not None
    assert result.type == ChatDataType.TEXT
    assert result.data == "你好，世界！"
    print("[通过] 文本消息解析")
    
    # 测试开始消息
    start_msg = json.dumps({
        "type": "start",
        "payload": {"session_id": "test-session-001"}
    })
    result = parser.parse_message(start_msg)
    assert result is not None
    assert result.metadata.get("session_id") == "test-session-001"
    print("[通过] 开始消息解析")
    
    # 测试完成消息
    complete_msg = json.dumps({
        "type": "complete",
        "payload": {}
    })
    result = parser.parse_message(complete_msg)
    assert result is not None
    assert result.type == ChatDataType.AVATAR_TEXT
    assert result.is_end == True
    print("[通过] 完成消息解析")
    
    # 测试打断消息
    interrupt_msg = json.dumps({
        "type": "interrupt",
        "payload": {}
    })
    result = parser.parse_message(interrupt_msg)
    assert result is not None
    assert result.type == ChatDataType.INTERRUPT
    print("[通过] 打断消息解析")
    
    # 测试无效消息
    invalid_msg = json.dumps({
        "type": "unknown",
        "payload": {}
    })
    result = parser.parse_message(invalid_msg)
    assert result is None
    print("[通过] 无效消息处理")
    
    print("\n所有测试通过！")


def test_full_conversation():
    """测试完整对话流程"""
    print("\n" + "=" * 60)
    print("测试 2: 完整对话流程")
    print("=" * 60)
    
    server = MockWebSocketServer()
    
    # 模拟完整对话
    conversation = [
        # 开始会话
        json.dumps({
            "type": "start",
            "payload": {"session_id": "conv-001"}
        }),
        # 分段发送文本
        json.dumps({
            "type": "text",
            "payload": {"text": "你好，"}
        }),
        json.dumps({
            "type": "text",
            "payload": {"text": "我是"}
        }),
        json.dumps({
            "type": "text",
            "payload": {"text": "数字人。"}
        }),
        # 完成
        json.dumps({
            "type": "complete",
            "payload": {}
        }),
    ]
    
    server.simulate_conversation(conversation)
    
    # 验证结果
    assert len(server.parser.text_buffer) == 0, "缓冲区应该已清空"
    print("\n[验证] 缓冲区已清空")
    print("[通过] 完整对话流程测试")


def test_interrupt_scenario():
    """测试打断场景"""
    print("\n" + "=" * 60)
    print("测试 3: 打断场景")
    print("=" * 60)
    
    server = MockWebSocketServer()
    
    # 模拟被打断的对话
    conversation = [
        json.dumps({"type": "start", "payload": {"session_id": "conv-002"}}),
        json.dumps({"type": "text", "payload": {"text": "这是一段很长的文本"}}),
        json.dumps({"type": "text", "payload": {"text": "但是还没说完"}}),
        # 突然打断
        json.dumps({"type": "interrupt", "payload": {}}),
        # 新对话
        json.dumps({"type": "text", "payload": {"text": "新的话题"}}),
        json.dumps({"type": "complete", "payload": {}}),
    ]
    
    server.simulate_conversation(conversation)
    
    # 验证结果
    assert len(server.parser.text_buffer) == 0, "缓冲区应该已清空"
    print("\n[验证] 打断后缓冲区已清空")
    print("[通过] 打断场景测试")


if __name__ == "__main__":
    print("=" * 60)
    print("Xiling WebSocket Handler Mock 测试")
    print("=" * 60)
    
    # 运行测试
    test_single_messages()
    test_full_conversation()
    test_interrupt_scenario()
    
    print("\n" + "=" * 60)
    print("所有测试通过！")
    print("=" * 60)
