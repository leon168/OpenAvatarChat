#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
远程测试客户端 - 测试公网 IP 连接
"""
import asyncio
import json
import ssl
import websockets


async def test_text_drive():
    # 远程服务器
    host = "117.50.193.120"
    port = 8282
    text = "你好，我是数字人，很高兴见到大家！"

    # 使用 ws:// 而不是 wss://，避免 SSL 证书问题
    uri = f"ws://{host}:{port}/ws/xiling?appId=test&token=test&liveRoom=test"
    print(f"Connecting to: {uri}")
    print(f"Server: {host}:{port}")
    print(f"Text: {text}")
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
                    stream_id = "stream_001"
                    text_msg = {
                        "id": 1,
                        "type": "TEXT",
                        "body": json.dumps({
                            "text": text,
                            "streamId": stream_id
                        }, ensure_ascii=False)
                    }
                    print(f"[SEND] Text message:")
                    print(f"  Stream ID: {stream_id}")
                    print(f"  Text: {text}")
                    print()
                    await ws.send(json.dumps(text_msg))
                    print("[SENT] Message sent successfully!")
                    print()

                    # 接收响应
                    print("Waiting for responses...")
                    timeout_counter = 0
                    while timeout_counter < 60:  # 60秒超时
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

                    if timeout_counter >= 60:
                        print("[TIMEOUT] No response after 60 seconds")

                else:
                    print(f"[ERROR] Expected READY, got: {data.get('type')}")

            except asyncio.TimeoutError:
                print("[ERROR] Timeout waiting for READY message")
                print("Server might not be responding correctly")

    except ConnectionRefusedError:
        print("[ERROR] Connection refused")
        print("Please check:")
        print("  1. Is server running?")
        print(f"  2. Is server listening on port {port}?")
        print("  3. Check firewall settings")
        print("  4. Try connecting from the same network")

    except OSError as e:
        print(f"[ERROR] Connection error: {e}")
        if "10061" in str(e) or "111" in str(e):
            print("  -> Port is closed or service not running")
        elif "110" in str(e):
            print("  -> Connection timeout")
            print("  -> Check network connectivity")

    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")
        import traceback
        traceback.print_exc()


async def test_wss_with_ssl():
    """测试 wss:// 连接并禁用 SSL 验证"""
    host = "117.50.193.120"
    port = 8282

    # 使用 wss:// 并禁用 SSL 验证
    uri = f"wss://{host}:{port}/ws/xiling?appId=test&token=test&liveRoom=test"
    print(f"Connecting to: {uri} (with SSL verification disabled)")
    print()

    try:
        # 创建 SSL 上下文，禁用验证
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        async with websockets.connect(uri, ssl=ssl_context, ping_timeout=10, close_timeout=10) as ws:
            print("[SUCCESS] Connected via WSS!")
            response = await ws.recv()
            data = json.loads(response)
            print(f"[RECEIVED] {json.dumps(data, ensure_ascii=False, indent=2)}")

    except Exception as e:
        print(f"[ERROR] WSS connection failed: {e}")


if __name__ == "__main__":
    import sys

    print("=" * 60)
    print("Xiling WebSocket Client Test")
    print("=" * 60)
    print()

    # 测试 ws:// 连接（推荐）
    print("Test 1: WS connection (no SSL)")
    print("-" * 60)
    asyncio.run(test_text_drive())

    print()
    print("=" * 60)
    print()

    # 如果需要，测试 wss:// 连接
    if len(sys.argv) > 1 and sys.argv[1] == "--wss":
        print("Test 2: WSS connection (SSL verification disabled)")
        print("-" * 60)
        asyncio.run(test_wss_with_ssl())
    else:
        print("To test WSS connection, run:")
        print("  python test_remote_client.py --wss")
