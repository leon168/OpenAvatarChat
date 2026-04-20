#!/usr/bin/env python3
"""
测试希灵 WebSocket 客户端
"""
import asyncio
import argparse
import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import websockets
import numpy as np


async def test_text_drive(uri: str, text: str):
    """测试文本驱动"""
    print(f"连接到: {uri}")
    
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


async def test_audio_drive(uri: str, audio_file: str):
    """测试音频驱动"""
    print(f"连接到: {uri}")
    
    async with websockets.connect(uri) as ws:
        print("已连接，等待 READY...")
        
        # 等待 READY
        response = await ws.recv()
        data = json.loads(response)
        print(f"收到: {data}")
        
        if data.get("type") != "READY":
            print("未收到 READY，退出")
            return
        
        # 发送 START
        start_msg = {
            "id": 1,
            "type": "START",
            "body": json.dumps({
                "streamId": "audio_stream_001",
                "event": "START",
                "sampleRate": 16000
            })
        }
        print(f"发送 START: {start_msg}")
        await ws.send(json.dumps(start_msg))
        
        # 等待 START 确认
        response = await ws.recv()
        data = json.loads(response)
        print(f"收到: {data}")
        
        # 读取并发送音频
        with open(audio_file, "rb") as f:
            audio_data = f.read()
        
        # 分块发送 (每块 320ms = 5120 bytes @ 16kHz 16bit mono)
        chunk_size = 5120
        for i in range(0, len(audio_data), chunk_size):
            chunk = audio_data[i:i+chunk_size]
            await ws.send(chunk)
            await asyncio.sleep(0.32)  # 320ms
        
        # 发送 COMPLETE
        complete_msg = {
            "id": 2,
            "type": "COMPLETE",
            "body": json.dumps({
                "streamId": "audio_stream_001",
                "event": "COMPLETE"
            })
        }
        print(f"发送 COMPLETE: {complete_msg}")
        await ws.send(json.dumps(complete_msg))
        
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


async def test_interrupt(uri: str):
    """测试打断"""
    print(f"连接到: {uri}")
    
    async with websockets.connect(uri) as ws:
        print("已连接")
        
        # 发送 INTERRUPT
        msg = {
            "id": 1,
            "type": "INTERRUPT",
            "body": json.dumps({})
        }
        print(f"发送: {msg}")
        await ws.send(json.dumps(msg))
        
        # 接收响应
        response = await ws.recv()
        data = json.loads(response)
        print(f"收到: {data}")


def main():
    parser = argparse.ArgumentParser(description="测试希灵 WebSocket 客户端")
    parser.add_argument("--host", default="localhost", help="服务器地址")
    parser.add_argument("--port", type=int, default=8282, help="服务器端口")
    parser.add_argument("--appId", default="test_app", help="应用ID")
    parser.add_argument("--token", default="test_token", help="令牌")
    parser.add_argument("--liveRoom", default="test_room", help="直播间名称")
    
    subparsers = parser.add_subparsers(dest="command", help="命令")
    
    # text 命令
    text_parser = subparsers.add_parser("text", help="文本驱动")
    text_parser.add_argument("--text", default="大家好，我是数字人", help="要播报的文本")
    
    # audio 命令
    audio_parser = subparsers.add_parser("audio", help="音频驱动")
    audio_parser.add_argument("--file", required=True, help="音频文件路径 (PCM 16kHz 16bit mono)")
    
    # interrupt 命令
    interrupt_parser = subparsers.add_parser("interrupt", help="打断")
    
    args = parser.parse_args()
    
    uri = f"ws://{args.host}:{args.port}/ws/xiling?appId={args.appId}&token={args.token}&liveRoom={args.liveRoom}"
    
    if args.command == "text":
        asyncio.run(test_text_drive(uri, args.text))
    elif args.command == "audio":
        asyncio.run(test_audio_drive(uri, args.file))
    elif args.command == "interrupt":
        asyncio.run(test_interrupt(uri))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
