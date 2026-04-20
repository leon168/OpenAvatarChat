#!/usr/bin/env python3
"""
简化版 WebSocket 测试客户端
"""
import asyncio
import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

import websockets


async def test_text_drive():
    host = "117.50.193.120"
    port = 8282
    text = "你好，我是数字人"

    # 使用 wss:// 因为服务器配置了 SSL
    uri = f"wss://{host}:{port}/ws/xiling?appId=test&token=test&liveRoom=test"
    print(f"连接到: {uri}")

    try:
        async with websockets.connect(uri) as ws:
            print("已连接，等待 READY...")

            # 等待 READY
            response = await ws.recv()
            data = json.loads(response)
            print(f"收到: {data}")

            if data.get("type") != "READY":
                print("未收到 READY，退出")
                return

            # 发送 TEXT 消息
            msg = {
                "id": 1,
                "type": "TEXT",
                "body": json.dumps({
                    "text": text,
                    "streamId": "stream_001"
                })
            }
            print(f"发送: {msg}")
            await ws.send(json.dumps(msg))

            # 接收响应
            while True:
                try:
                    response = await ws.recv()
                    data = json.loads(response)
                    print(f"收到: {data}")

                    if data.get("type") in ["COMPLETE", "ERROR"]:
                        break
                except websockets.exceptions.ConnectionClosed:
                    print("连接已关闭")
                    break

    except Exception as e:
        print(f"错误: {e}")


if __name__ == "__main__":
    asyncio.run(test_text_drive())
