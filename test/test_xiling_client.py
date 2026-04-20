#!/usr/bin/env python3
"""
测试希灵 WebSocket 客户端 - 支持文本驱动数字人播报

用法:
  python test_xiling_client.py text --text "你好" --wait 10
  python test_xiling_client.py text --text "你好" --ws
"""
import asyncio
import argparse
import json
import ssl
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import websockets


async def test_text_drive(uri: str, text: str, wait_time: int = 15, use_ssl: bool = True):
    """测试文本驱动"""
    print(f"连接到: {uri}")

    ssl_context = None
    if use_ssl:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

    try:
        async with websockets.connect(
            uri,
            ssl=ssl_context,
            ping_timeout=20,
            close_timeout=10,
            max_size=10 * 1024 * 1024,
        ) as ws:
            print("已连接，等待 READY...")

            # 等待 READY
            response = await asyncio.wait_for(ws.recv(), timeout=10)
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
                }, ensure_ascii=False)
            }
            print(f"发送 TEXT: {text[:50]}...")
            await ws.send(json.dumps(msg))

            # 接收协议响应 (START/COMPLETE 是 Xiling 协议确认，不是播报完成)
            print("等待协议确认...")
            while True:
                try:
                    response = await asyncio.wait_for(ws.recv(), timeout=5)
                    data = json.loads(response)
                    msg_type = data.get("type")
                    print(f"  收到 {msg_type}")

                    if msg_type == "COMPLETE":
                        print("文本已被服务端接收，数字人正在播报...")
                        break
                    elif msg_type == "ERROR":
                        print(f"服务端错误: {data}")
                        return
                except asyncio.TimeoutError:
                    print("  等待超时...")
                    break

            # 保持连接，等待数字人播报完成
            print(f"保持连接 {wait_time} 秒，等待数字人播报...")
            remaining = wait_time
            while remaining > 0:
                try:
                    response = await asyncio.wait_for(ws.recv(), timeout=1)
                    data = json.loads(response)
                    print(f"  收到: {data.get('type', 'unknown')}")
                except asyncio.TimeoutError:
                    remaining -= 1
                    if remaining % 5 == 0:
                        print(f"  还在播报中... ({remaining}s)")

            print("测试完成，关闭连接")

    except ConnectionRefusedError:
        print(f"连接被拒绝，请检查服务是否在运行: {uri}")
    except Exception as e:
        print(f"连接错误: {e}")


async def test_multi_text(uri: str, texts: list, wait_between: float = 3.0, use_ssl: bool = True):
    """测试多条文本驱动"""
    print(f"连接到: {uri}")

    ssl_context = None
    if use_ssl:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

    try:
        async with websockets.connect(
            uri,
            ssl=ssl_context,
            ping_timeout=20,
            close_timeout=10,
            max_size=10 * 1024 * 1024,
        ) as ws:
            print("已连接，等待 READY...")

            # 等待 READY
            response = await asyncio.wait_for(ws.recv(), timeout=10)
            data = json.loads(response)
            if data.get("type") != "READY":
                print("未收到 READY，退出")
                return

            print("服务端已就绪")

            for i, text in enumerate(texts, 1):
                stream_id = f"stream_{i:03d}"
                print(f"\n--- 第 {i}/{len(texts)} 条 ---")
                print(f"文本: {text[:50]}...")

                # 发送 TEXT
                msg = {
                    "id": i,
                    "type": "TEXT",
                    "body": json.dumps({
                        "text": text,
                        "streamId": stream_id
                    }, ensure_ascii=False)
                }
                await ws.send(json.dumps(msg))

                # 等待协议确认
                try:
                    while True:
                        response = await asyncio.wait_for(ws.recv(), timeout=5)
                        data = json.loads(response)
                        if data.get("type") in ["COMPLETE", "ERROR"]:
                            break
                except asyncio.TimeoutError:
                    pass

                # 等待播报（估算时间）
                if i < len(texts):
                    print(f"等待 {wait_between}s 后发送下一条...")
                    await asyncio.sleep(wait_between)

            # 发送完所有文本后保持连接
            print("\n所有文本已发送，保持连接 15 秒等待播报完成...")
            remaining = 15
            while remaining > 0:
                try:
                    response = await asyncio.wait_for(ws.recv(), timeout=1)
                except asyncio.TimeoutError:
                    remaining -= 1

            print("测试完成")

    except Exception as e:
        print(f"错误: {e}")


def main():
    parser = argparse.ArgumentParser(description="测试希灵 WebSocket 客户端")
    parser.add_argument("--host", default="localhost", help="服务器地址")
    parser.add_argument("--port", type=int, default=8282, help="服务器端口")
    parser.add_argument("--appId", default="test_app", help="应用ID")
    parser.add_argument("--token", default="test_token", help="令牌")
    parser.add_argument("--liveRoom", default="test_room", help="直播间名称")
    parser.add_argument("--ws", action="store_true", help="使用 ws:// (不加密)")
    parser.add_argument("--wss", action="store_true", default=True, help="使用 wss:// (加密)")

    subparsers = parser.add_subparsers(dest="command", help="命令")

    # text 命令
    text_parser = subparsers.add_parser("text", help="文本驱动")
    text_parser.add_argument("--text", default="大家好，我是数字人", help="要播报的文本")
    text_parser.add_argument("--wait", type=int, default=15, help="发送后保持连接的秒数")

    # multi 命令
    multi_parser = subparsers.add_parser("multi", help="多条文本驱动")

    # interrupt 命令
    subparsers.add_parser("interrupt", help="打断")

    args = parser.parse_args()

    use_ssl = not args.ws
    protocol = "wss" if use_ssl else "ws"
    uri = f"{protocol}://{args.host}:{args.port}/ws/xiling?appId={args.appId}&token={args.token}&liveRoom={args.liveRoom}"

    if args.command == "text":
        asyncio.run(test_text_drive(uri, args.text, args.wait, use_ssl))
    elif args.command == "multi":
        texts = [
            "你好，我是数字人，很高兴见到大家！",
            "今天天气真不错，适合出去走走。",
            "我是基于人工智能技术的数字人助手。",
        ]
        asyncio.run(test_multi_text(uri, texts, use_ssl=use_ssl))
    elif args.command == "interrupt":
        print("暂未实现")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
