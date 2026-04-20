#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
完整的 Xiling WebSocket 测试客户端
"""
import asyncio
import json
import ssl
import websockets


async def test_xiling_client(host="117.50.193.120", port=8282, use_ssl=True):
    """测试 Xiling WebSocket 客户端"""

    # 构造 URI
    protocol = "wss" if use_ssl else "ws"
    uri = f"{protocol}://{host}:{port}/ws/xiling?appId=test&token=test&liveRoom=test"

    print("=" * 60)
    print("Xiling WebSocket Client Test")
    print("=" * 60)
    print(f"Server: {host}:{port}")
    print(f"Protocol: {protocol}")
    print(f"URI: {uri}")
    print()

    # SSL 配置
    ssl_context = None
    if use_ssl:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        print("SSL verification: DISABLED (for testing)")
        print()

    try:
        async with websockets.connect(
            uri,
            ssl=ssl_context,
            ping_timeout=20,
            close_timeout=20,
            max_size=10 * 1024 * 1024  # 10MB
        ) as ws:
            print("[SUCCESS] Connected to server!")
            print()

            # 等待 READY 消息
            print("Waiting for READY message...")
            try:
                response = await asyncio.wait_for(ws.recv(), timeout=10)
                data = json.loads(response)
                msg_type = data.get("type")

                if msg_type == "READY":
                    print(f"[OK] Server is ready!")
                    print(f"  Message: {json.dumps(data, ensure_ascii=False, indent=4)}")
                    print()

                    # 发送测试文本
                    test_texts = [
                        "你好，我是数字人，很高兴见到大家！",
                        "今天天气真不错，适合出去走走。",
                        "我是基于人工智能技术的数字人助手。"
                    ]

                    for i, text in enumerate(test_texts, 1):
                        stream_id = f"stream_{i:03d}"
                        print(f"\n{'='*60}")
                        print(f"Test {i}/{len(test_texts)}: {text}")
                        print(f"{'='*60}")

                        # 发送 TEXT 消息
                        text_msg = {
                            "id": i,
                            "type": "TEXT",
                            "body": json.dumps({
                                "text": text,
                                "streamId": stream_id
                            }, ensure_ascii=False)
                        }

                        print(f"[SEND] Sending TEXT message...")
                        await ws.send(json.dumps(text_msg))
                        print(f"[SENT] Message sent successfully!")
                        print()

                        # 接收响应
                        print("[WAIT] Waiting for responses...")
                        received_complete = False
                        timeout_counter = 0

                        while timeout_counter < 30 and not received_complete:
                            try:
                                response = await asyncio.wait_for(ws.recv(), timeout=1)
                                data = json.loads(response)
                                msg_type = data.get("type")

                                print(f"[RECEIVED] {msg_type}:")
                                print(f"  {json.dumps(data, ensure_ascii=False, indent=2)}")
                                print()

                                if msg_type in ["COMPLETE", "ERROR"]:
                                    received_complete = True
                                    if msg_type == "ERROR":
                                        print(f"[ERROR] Server returned an error")
                                    else:
                                        print(f"[OK] Text processing completed!")
                                elif msg_type == "START":
                                    print(f"[INFO] Processing started...")

                            except asyncio.TimeoutError:
                                timeout_counter += 1
                                if timeout_counter % 5 == 0:
                                    print(f"[WAIT] Still waiting... ({timeout_counter}s)")

                        if not received_complete:
                            print(f"[TIMEOUT] No response after 30 seconds")

                        # 等待一下再发送下一条
                        if i < len(test_texts):
                            await asyncio.sleep(2)

                    # 测试完成
                    print(f"\n{'='*60}")
                    print("All tests completed!")
                    print(f"{'='*60}")

                else:
                    print(f"[ERROR] Expected READY, got: {msg_type}")
                    print(f"  Full message: {json.dumps(data, ensure_ascii=False, indent=4)}")

            except asyncio.TimeoutError:
                print("[ERROR] Timeout waiting for READY message")
                print("Server might not be responding correctly")

    except ConnectionRefusedError:
        print("[ERROR] Connection refused")
        print("Please check:")
        print(f"  1. Is server running on {host}:{port}?")
        print("  2. Is the port open? (check firewall)")
        print("  3. Try connecting from the same network")

    except OSError as e:
        print(f"[ERROR] Connection error: {e}")

    except websockets.exceptions.InvalidMessage as e:
        print(f"[ERROR] Invalid message: {e}")
        print("This might mean:")
        print("  - Server is using HTTPS but you connected with HTTP")
        print("  - Server is not running")
        print("  - Wrong protocol (try --ws or --wss)")

    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    import sys

    # 默认配置
    host = "117.50.193.120"
    port = 8282
    use_ssl = True  # 默认使用 WSS

    # 解析命令行参数
    if len(sys.argv) > 1:
        if sys.argv[1] == "--ws":
            use_ssl = False
        elif sys.argv[1] == "--wss":
            use_ssl = True
        elif sys.argv[1] == "--help":
            print("Usage:")
            print("  python test_xiling_complete.py [--ws|--wss]")
            print()
            print("Options:")
            print("  --ws   Use ws:// (no SSL)")
            print("  --wss  Use wss:// (with SSL, default)")
            sys.exit(0)

    # 运行测试
    asyncio.run(test_xiling_client(host=host, port=port, use_ssl=use_ssl))
