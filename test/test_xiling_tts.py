#!/usr/bin/env python3
"""
测试希灵 TTS 客户端
直接调用百度流式语音合成 WebSocket API
"""
import asyncio
import argparse
import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import websockets
import numpy as np


async def test_tts(api_key: str, text: str, per: str = "5118"):
    """
    测试百度流式 TTS
    
    发音人列表:
    - 5118: 度小宇 (男声)
    - 5119: 度小美 (女声)
    - 5120: 度逍遥 (男声)
    - 5121: 度丫丫 (女童)
    - 5122: 度博文 (男声)
    - 5123: 度小贤 (男声)
    - 5124: 度小鹿 (女童)
    - 5125: 度灵灵 (女声)
    - 5126: 度姗姗 (女声)
    - 5127: 度墨墨 (男声)
    - 5128: 度潇潇 (女声)
    - 5003: 度逍遥 (精品)
    - 5116: 度小美 (精品)
    - 5117: 度博文 (精品)
    """
    # 获取 access_token
    import requests
    
    # 使用 API Key 获取 token
    token_url = "https://aip.baidubce.com/oauth/2.0/token"
    params = {
        "grant_type": "client_credentials",
        "client_id": api_key,
        "client_secret": ""  # 需要填写 Secret
    }
    
    print(f"获取 access_token...")
    # 注意：这里需要完整的 API Key + Secret
    # 实际使用时请填写正确的 client_secret
    
    # WebSocket 连接
    uri = f"wss://aip.baidubce.com/ws/2.0/speech/publiccloudspeech/v1/tts?access_token=YOUR_TOKEN&per={per}"
    
    print(f"连接到: {uri}")
    print(f"合成文本: {text}")
    
    async with websockets.connect(uri) as ws:
        print("已连接")
        
        # 发送开始合成帧
        start_msg = {
            "type": "system.start",
            "payload": {}
        }
        print(f"发送开始帧: {start_msg}")
        await ws.send(json.dumps(start_msg))
        
        # 等待 started 响应
        response = await ws.recv()
        data = json.loads(response)
        print(f"收到: {data}")
        
        if data.get("type") != "system.started" or data.get("code") != 0:
            print("开始合成失败")
            return
        
        # 发送文本
        text_msg = {
            "type": "system.data",
            "payload": {
                "text": text
            }
        }
        print(f"发送文本: {text_msg}")
        await ws.send(json.dumps(text_msg))
        
        # 接收音频数据
        audio_data = bytearray()
        while True:
            try:
                response = await ws.recv()
                
                # 检查是否为二进制音频数据
                if isinstance(response, bytes):
                    audio_data.extend(response)
                    print(f"收到音频数据: {len(response)} bytes")
                else:
                    # JSON 响应
                    data = json.loads(response)
                    print(f"收到: {data}")
                    
                    if data.get("type") == "system.finish":
                        print("合成完成")
                        break
                    elif data.get("type") == "system.error":
                        print(f"错误: {data}")
                        break
                        
            except websockets.exceptions.ConnectionClosed:
                print("连接已关闭")
                break
        
        # 保存音频
        if audio_data:
            output_file = "test_output.pcm"
            with open(output_file, "wb") as f:
                f.write(audio_data)
            print(f"音频已保存: {output_file} ({len(audio_data)} bytes)")


def main():
    parser = argparse.ArgumentParser(description="测试百度流式 TTS")
    parser.add_argument("--api-key", required=True, help="百度 API Key")
    parser.add_argument("--secret", required=True, help="百度 Secret Key")
    parser.add_argument("--text", default="你好，这是百度流式语音合成测试", help="要合成的文本")
    parser.add_argument("--per", default="5118", help="发音人ID")
    
    args = parser.parse_args()
    
    # 先获取 access_token
    import requests
    token_url = "https://aip.baidubce.com/oauth/2.0/token"
    params = {
        "grant_type": "client_credentials",
        "client_id": args.api_key,
        "client_secret": args.secret
    }
    
    print(f"获取 access_token...")
    response = requests.post(token_url, params=params)
    token_data = response.json()
    
    if "access_token" not in token_data:
        print(f"获取 token 失败: {token_data}")
        return
    
    access_token = token_data["access_token"]
    print(f"获取 token 成功")
    
    # 使用 token 运行 WebSocket TTS
    uri = f"wss://aip.baidubce.com/ws/2.0/speech/publiccloudspeech/v1/tts?access_token={access_token}&per={args.per}"
    
    asyncio.run(run_tts(uri, args.text))


async def run_tts(uri: str, text: str):
    """运行 TTS"""
    print(f"连接到: {uri[:80]}...")
    print(f"合成文本: {text}")
    
    async with websockets.connect(uri) as ws:
        print("已连接")
        
        # 发送开始合成帧
        start_msg = {
            "type": "system.start",
            "payload": {}
        }
        await ws.send(json.dumps(start_msg))
        
        # 等待 started 响应
        response = await ws.recv()
        data = json.loads(response)
        print(f"收到: {data}")
        
        if data.get("type") != "system.started" or data.get("code") != 0:
            print("开始合成失败")
            return
        
        # 发送文本
        text_msg = {
            "type": "system.data",
            "payload": {
                "text": text
            }
        }
        await ws.send(json.dumps(text_msg))
        
        # 接收音频数据
        audio_data = bytearray()
        while True:
            try:
                response = await ws.recv()
                
                # 检查是否为二进制音频数据
                if isinstance(response, bytes):
                    audio_data.extend(response)
                    print(f"收到音频数据: {len(response)} bytes, 总计: {len(audio_data)} bytes")
                else:
                    # JSON 响应
                    data = json.loads(response)
                    print(f"收到消息: {data.get('type')}")
                    
                    if data.get("type") == "system.finish":
                        print("合成完成")
                        break
                    elif data.get("type") == "system.error":
                        print(f"错误: {data}")
                        break
                        
            except websockets.exceptions.ConnectionClosed:
                print("连接已关闭")
                break
        
        # 发送结束帧
        finish_msg = {
            "type": "system.finish"
        }
        await ws.send(json.dumps(finish_msg))
        
        # 等待 finished 响应
        try:
            response = await asyncio.wait_for(ws.recv(), timeout=5.0)
            data = json.loads(response)
            print(f"结束响应: {data}")
        except asyncio.TimeoutError:
            print("等待结束响应超时")
        
        # 保存音频
        if audio_data:
            output_file = "test_output.pcm"
            with open(output_file, "wb") as f:
                f.write(audio_data)
            print(f"音频已保存: {output_file}")
            print(f"总大小: {len(audio_data)} bytes")
            print(f"时长: {len(audio_data) / 32000:.2f} 秒 (16kHz 16bit mono)")


if __name__ == "__main__":
    main()
