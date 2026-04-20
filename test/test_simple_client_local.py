#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
本地测试客户端
"""
import asyncio
import json
import websockets


async def test_text_drive():
    # 使用 localhost
    host = "localhost"
    port = 8282
    text = "你好，我是数字人"

    # 尝试 ws:// (非加密)
    uri = f"ws://{host}:{port}/ws/xiling?appId=test&token=test&liveRoom=test"
    print(f"Connecting to: {uri}")

    try:
        async with websockets.connect(uri) as ws:
            print("Connected, waiting for READY...")

            # Wait for READY
            response = await ws.recv()
            data = json.loads(response)
            print(f"Received: {data}")

            if data.get("type") != "READY":
                print("Did not receive READY, exiting")
                return

            # Send TEXT message
            msg = {
                "id": 1,
                "type": "TEXT",
                "body": json.dumps({
                    "text": text,
                    "streamId": "stream_001"
                })
            }
            print(f"Sending: {msg}")
            await ws.send(json.dumps(msg))

            # Receive responses
            while True:
                try:
                    response = await ws.recv()
                    data = json.loads(response)
                    print(f"Received: {data}")

                    if data.get("type") in ["COMPLETE", "ERROR"]:
                        break
                except websockets.exceptions.ConnectionClosed:
                    print("Connection closed")
                    break

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(test_text_drive())
