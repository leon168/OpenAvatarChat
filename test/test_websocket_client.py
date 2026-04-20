# -*- coding: utf-8 -*-
import asyncio
import json
import ssl
import websockets

class XilingWebSocketClient:
    def __init__(self, uri='wss://localhost:8282/ws/xiling'):
        self.uri = uri
        self.websocket = None
        self.connected = False

    async def connect(self):
        ssl_context = ssl._create_unverified_context()
        self.websocket = await websockets.connect(self.uri, ssl=ssl_context)
        self.connected = True
        print('[+] WebSocket 连接成功')

    async def send_text(self, text):
        if not self.connected:
            raise Exception('未连接')
        message = {'type': 'text', 'payload': {'text': text}}
        await self.websocket.send(json.dumps(message))
        print(f'[+] 已发送: {text}')

    async def close(self):
        if self.websocket:
            await self.websocket.close()
            self.connected = False
            print('[+] 连接已关闭')

async def main():
    client = XilingWebSocketClient()
    try:
        await client.connect()
        await client.send_text('你好，欢迎体验数字人直播系统')
        await asyncio.sleep(2)
        await client.send_text('这是第二段测试文本')
    finally:
        await client.close()

if __name__ == '__main__':
    asyncio.run(main())
