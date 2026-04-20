#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
最终版 WebSocket 测试客户端
"""
import asyncio
import json
import websockets


async def test_websocket():
    # 配置
    host = "localhost"  # 本地测试用 localhost，远程测试改为 "117.50.193.120"
    port = 8282
    path = "/ws/xiling"

    # 查询参数
    params = {
        "appId": "test_app",
        "token": "test_token",
        "liveRoom": "test_room"
    }

    # 构造 URI
    uri = f"ws://{host}:{port}{path}?" + "&".join(f"{k}={v}" for k, v in params.items())
    print(f"Connecting to:")
    print(f"  Host: {host}:{port}")
    print(f"  Path: {path}")
    print(f"  URI: {uri}")
    print()

    try:
        async with websockets.connect(uri, ping_timeout=10, close_timeout=10) as ws:
            print("[SUCCESS] Connected successfully!")
            print()

            # 等待 READY 消息
            print("Waiting for READY message...")
            try:
                response = await asyncio.wait_for(ws.recv(), timeout=5)
                data = json.loads(response)
                print(f"[RECEIVED] {json.dumps(data, ensure_ascii=False, indent=2)}")
                print()

                if data.get("type") == "READY":
                    print("[OK] Server is ready!")
                    print()

                    # 发送测试文本
                    test_text = "你好，我是数字人，很高兴见到大家！"
                    stream_id = "stream_001"

                    print(f"Sending text: {test_text}")
                    text_msg = {
                        "id": 1,
                        "type": "TEXT",
                        "body": json.dumps({
                            "text": test_text,
                            "streamId": stream_id
                        }, ensure_ascii=False)
                    }
                    await ws.send(json.dumps(text_msg))
                    print("[SENT] Text message sent")
                    print()

                    # 接收响应
                    print("Waiting for responses...")
                    timeout_counter = 0
                    while timeout_counter < 30:  # 30秒超时
                        try:
                            response = await asyncio.wait_for(ws.recv(), timeout=1)
                            data = json.loads(response)
                            msg_type = data.get("type")
                            print(f"[RECEIVED] {msg_type}: {json.dumps(data, ensure_ascii=False, indent=2)}")
                            print()

                            if msg_type in ["COMPLETE", "ERROR"]:
                                print(f"[DONE] Process finished with type: {msg_type}")
                                break

                        except asyncio.TimeoutError:
                            timeout_counter += 1
                            if timeout_counter % 5 == 0:
                                print(f"[WAIT] Waiting... ({timeout_counter}s)")

                    if timeout_counter >= 30:
                        print("[TIMEOUT] No response after 30 seconds")

                else:
                    print(f"[ERROR] Expected READY, got: {data.get('type')}")

            except asyncio.TimeoutError:
                print("[ERROR] Timeout waiting for READY message")
                print("Server might not be responding correctly")

    except websockets.exceptions.InvalidStatusCode as e:
        print(f"[ERROR] Invalid status code: {e.status_code}")
        print(f"  Headers: {e.headers}")
        if e.status_code == 426:
            print("  Tip: Server might require WSS instead of WS (SSL)")

    except websockets.exceptions.ConnectionClosed as e:
        print(f"[ERROR] Connection closed: {e}")
        print(f"  Code: {e.code}")
        print(f"  Reason: {e.reason}")

    except ConnectionRefusedError:
        print("[ERROR] Connection refused")
        print("Please check:")
        print("  1. Is the server running?")
        print(f"  2. Is the server listening on port {port}?")
        print("  3. Check with: netstat -ano | findstr :8282")

    except OSError as e:
        print(f"[ERROR] Connection error: {e}")
        if "10061" in str(e):
            print("  -> Port is closed or service not running")

    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")
        import traceback
        traceback.print_exc()


async def test_remote():
    """测试远程服务器"""
    print("=" * 60)
    print("Testing REMOTE server")
    print("=" * 60)

    host = "117.50.193.120"
    port = 8282
    uri = f"ws://{host}:8282/ws/xiling?appId=test&token=test&liveRoom=test"

    print(f"Connecting to: {uri}")
    print()

    try:
        async with websockets.connect(uri, ping_timeout=5, close_timeout=5) as ws:
            print("[SUCCESS] Connected to remote server!")
            response = await ws.recv()
            data = json.loads(response)
            print(f"[RECEIVED] {json.dumps(data, ensure_ascii=False, indent=2)}")

    except Exception as e:
        print(f"[FAILED] {type(e).__name__}: {e}")


if __name__ == "__main__":
    print("=" * 60)
    print("WebSocket Client Test")
    print("=" * 60)
    print()

    # 测试本地服务器
    asyncio.run(test_websocket())

    print()
    print("=" * 60)

    # 测试远程服务器
    asyncio.run(test_remote())
