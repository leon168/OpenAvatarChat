"""
测试脚本：验证百度智能云流式 TTS Handler

使用方法:
1. 设置环境变量: export XILING_API_KEY="your_api_key"
2. 运行测试: python test/test_xiling_tts_handler.py

功能:
- 测试 WebSocket 连接
- 测试文本转语音
- 保存音频输出到文件

交互流程（按官方 Demo）:
1. 握手建立连接
2. 发送 system.start 初始化
3. 等待 system.started
4. 发送文本 (type: text)
5. 接收音频数据
6. 发送 system.finish
7. 等待 system.finished
8. 断开连接
"""

import asyncio
import json
import os
import sys
import time
from urllib.parse import urlencode

import websockets

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class XilingTTSClient:
    """百度智能云流式 TTS 客户端"""
    
    WS_URL = "wss://aip.baidubce.com/ws/2.0/speech/publiccloudspeech/v1/tts"
    
    def __init__(self, api_key: str, per: str = "5118", spd: int = 5, 
                 pit: int = 5, vol: int = 5, aue: int = 3):
        self.api_key = api_key
        self.per = per
        self.spd = spd
        self.pit = pit
        self.vol = vol
        self.aue = aue
        self.audio_data = bytearray()
        self.session_id = None
        
    async def synthesize(self, text: str, output_file: str = None):
        """
        合成文本为语音（按官方 Demo 流程）
        """
        # URL 只带 per 参数
        params = {
            "per": self.per,
        }
        
        # 使用 API Key 鉴权
        extra_headers = {
            "Authorization": f"Bearer {self.api_key}"
        }
        
        url = f"{self.WS_URL}?{urlencode(params)}"
        
        print(f"[*] 步骤1: 握手建立连接...")
        print(f"[*] URL: {url}")
        print(f"[*] 文本长度: {len(text)} 字符")
        
        try:
            async with websockets.connect(url, additional_headers=extra_headers) as ws:
                print("[+] WebSocket 连接成功 (101 Switching Protocols)")
                
                # 步骤2: 发送 system.start 初始化
                print("\n[*] 步骤2: 发送 system.start 初始化...")
                start_payload = {
                    "type": "system.start",
                    "payload": {
                        "spd": self.spd,
                        "pit": self.pit,
                        "vol": self.vol,
                        "audio_ctrl": "{\"sampling_rate\":16000}",
                        "aue": self.aue
                    }
                }
                await ws.send(json.dumps(start_payload))
                print(f"[+] 已发送 system.start: {json.dumps(start_payload, ensure_ascii=False)}")
                
                # 步骤3: 等待 system.started
                print("\n[*] 步骤3: 等待 system.started...")
                init_msg = await asyncio.wait_for(ws.recv(), timeout=10.0)
                
                if isinstance(init_msg, str):
                    data = json.loads(init_msg)
                    msg_type = data.get("type", "")
                    code = data.get("code", -1)
                    message = data.get("message", "")
                    
                    if msg_type == "system.started":
                        if code == 0:
                            self.session_id = data.get("headers", {}).get("session_id", "")
                            print(f"[+] 初始化成功: {message}")
                            print(f"    session_id: {self.session_id}")
                        else:
                            raise Exception(f"初始化失败: code={code}, message={message}")
                    elif msg_type == "system.error":
                        raise Exception(f"初始化错误: code={code}, message={message}")
                    else:
                        raise Exception(f"未知的初始化响应: {data}")
                else:
                    raise Exception(f"初始化响应格式错误: 期望文本消息，收到二进制")
                
                # 步骤4: 发送文本
                print("\n[*] 步骤4: 发送文本...")
                text_payload = {
                    "type": "text",
                    "payload": {"text": text}
                }
                await ws.send(json.dumps(text_payload))
                print(f"[+] 已发送文本: {text[:50]}{'...' if len(text) > 50 else ''}")
                
                # 步骤5: 接收音频数据
                print("\n[*] 步骤5: 接收音频数据...")
                self.audio_data = bytearray()
                start_time = time.time()
                audio_started = False
                
                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        
                        if isinstance(msg, bytes):
                            # 二进制音频数据
                            self.audio_data.extend(msg)
                            if not audio_started:
                                audio_started = True
                                print(f"[+] 开始接收音频数据...")
                            print(f"    收到音频: {len(msg)} bytes, 总计: {len(self.audio_data)} bytes", end='\r')
                            
                        elif isinstance(msg, str):
                            # 文本控制消息
                            data = json.loads(msg)
                            msg_type = data.get("type", "")
                            
                            if msg_type == "system.error":
                                code = data.get("code", -1)
                                message = data.get("message", "")
                                print(f"\n[!] 服务端错误: code={code}, message={message}")
                                raise Exception(f"TTS Error: code={code}, message={message}")
                            else:
                                print(f"\n[*] 收到消息: {data}")
                                # 如果收到非错误消息，可能是结束信号
                                break
                                
                    except asyncio.TimeoutError:
                        if audio_started:
                            print(f"\n[+] 音频数据接收完成（超时）")
                            break
                        else:
                            print("\n[!] 等待音频数据超时")
                            raise Exception("等待音频数据超时")
                
                # 步骤6: 发送结束合成
                print("\n[*] 步骤6: 发送 system.finish...")
                finish_payload = {"type": "system.finish"}
                await ws.send(json.dumps(finish_payload))
                print("[+] 已发送 system.finish")
                
                # 步骤7: 等待 system.finished
                print("\n[*] 步骤7: 等待 system.finished...")
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=10.0)
                    if isinstance(msg, str):
                        data = json.loads(msg)
                        msg_type = data.get("type", "")
                        code = data.get("code", -1)
                        message = data.get("message", "")
                        
                        if msg_type == "system.finished":
                            if code == 0:
                                print(f"[+] 合成完成: {message}")
                            else:
                                print(f"[!] 合成完成但出错: code={code}, message={message}")
                        elif msg_type == "system.error":
                            print(f"[!] 合成错误: code={code}, message={message}")
                        else:
                            print(f"[*] 收到消息: {data}")
                except asyncio.TimeoutError:
                    print("[!] 等待 system.finished 超时")
                
                elapsed = time.time() - start_time
                print(f"\n[+] 音频接收完成，耗时: {elapsed:.2f}s")
                print(f"[+] 总音频大小: {len(self.audio_data)} bytes")
                
                # 步骤8: 断开连接
                print("\n[*] 步骤8: 断开连接...")
                await ws.close()
                print("[+] 连接已关闭")
                
                # 保存到文件
                if output_file and len(self.audio_data) > 0:
                    await self._save_audio(output_file)
                elif len(self.audio_data) == 0:
                    print("[!] 警告: 未收到音频数据")
                    
        except websockets.exceptions.InvalidStatusCode as e:
            status_code = e.status_code
            if status_code == 400:
                raise Exception(f"握手失败 (400 Bad Request): 参数错误或缺失")
            elif status_code == 401:
                raise Exception(f"握手失败 (401 Unauthorized): 鉴权失败，请检查 API Key")
            elif status_code == 403:
                raise Exception(f"握手失败 (403 Forbidden): 无访问权限，接口功能未开通")
            elif status_code == 404:
                raise Exception(f"握手失败 (404 Not Found): URL 错误")
            elif status_code == 429:
                raise Exception(f"握手失败 (429 Too Many Requests): 触发限流")
            elif status_code == 500:
                raise Exception(f"握手失败 (500 Internal Server Error): 服务器内部错误")
            elif status_code == 502:
                raise Exception(f"握手失败 (502 Bad Request): 后端服务连接失败")
            else:
                raise Exception(f"握手失败 (HTTP {status_code})")
                
        except websockets.exceptions.ConnectionClosed as e:
            if len(self.audio_data) > 0:
                print(f"[!] 连接意外关闭，但已收到 {len(self.audio_data)} bytes 音频")
                if output_file:
                    await self._save_audio(output_file)
            else:
                raise Exception(f"连接关闭: code={e.code}, reason={e.reason}")
                
        except Exception as e:
            raise Exception(f"TTS 处理失败: {e}")
                
        return bytes(self.audio_data)
    
    async def _save_audio(self, output_file: str):
        """保存音频数据到文件"""
        # 根据 aue 确定文件扩展名
        ext_map = {3: "mp3", 4: "pcm", 5: "pcm", 6: "wav"}
        ext = ext_map.get(self.aue, "bin")
        
        if not output_file.endswith(f".{ext}"):
            output_file = f"{output_file}.{ext}"
            
        with open(output_file, 'wb') as f:
            f.write(self.audio_data)
        
        print(f"[+] 音频已保存到: {output_file}")


def load_config():
    """从配置文件加载 TTS 配置"""
    import yaml
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                               "config", "chat_with_openai_compatible_xiling.yaml")
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    # 获取 XilingTTS 配置
    tts_config = config.get('default', {}).get('handler_configs', {}).get('XilingTTS', {})
    return tts_config


async def main():
    """主测试函数"""
    print("=" * 60)
    print("百度智能云流式 TTS Handler 测试")
    print("=" * 60)
    
    # 1. 检查环境变量
    api_key = os.environ.get("XILING_API_KEY")
    if not api_key:
        print("\n[!] 错误: 未设置 XILING_API_KEY 环境变量")
        print("    请设置: export XILING_API_KEY='your_api_key'")
        sys.exit(1)
    
    print(f"\n[+] API Key: {api_key[:8]}...{api_key[-4:]}")
    
    # 2. 加载配置
    try:
        config = load_config()
        print(f"\n[+] 已加载配置:")
        print(f"    - per (发音人): {config.get('per', '5118')}")
        print(f"    - spd (语速): {config.get('spd', 5)}")
        print(f"    - pit (音调): {config.get('pit', 5)}")
        print(f"    - vol (音量): {config.get('vol', 5)}")
        print(f"    - aue (格式): {config.get('aue', 3)} (3=mp3)")
        print(f"    - sample_rate: {config.get('sample_rate', 16000)} Hz")
    except Exception as e:
        print(f"\n[!] 加载配置失败: {e}")
        print("    使用默认配置")
        config = {}
    
    # 3. 创建 TTS 客户端
    client = XilingTTSClient(
        api_key=api_key,
        per=config.get('per', '5118'),
        spd=config.get('spd', 5),
        pit=config.get('pit', 5),
        vol=config.get('vol', 5),
        aue=config.get('aue', 3)
    )
    
    # 4. 测试文本
    test_texts = [
        "欢迎体验百度流式文本在线合成。",
        "这是一段测试文本，用于验证 TTS 功能是否正常工作。",
        "今天天气真不错，适合出去散步。"
    ]
    
    # 5. 执行测试
    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
    os.makedirs(output_dir, exist_ok=True)
    
    print("\n" + "-" * 60)
    print("开始 TTS 测试")
    print("-" * 60)
    
    success_count = 0
    for i, text in enumerate(test_texts, 1):
        print(f"\n{'='*60}")
        print(f"测试 {i}/{len(test_texts)}")
        print(f"{'='*60}")
        try:
            output_file = os.path.join(output_dir, f"xiling_tts_test_{i}")
            audio_data = await client.synthesize(text, output_file=output_file)
            if len(audio_data) > 0:
                print(f"\n[OK] 测试 {i} 通过")
                success_count += 1
            else:
                print(f"\n[FAIL] 测试 {i} 失败: 未收到音频数据")
        except Exception as e:
            print(f"\n[FAIL] 测试 {i} 失败: {e}")
    
    print("\n" + "=" * 60)
    print("测试完成")
    print(f"通过: {success_count}/{len(test_texts)}")
    print(f"音频文件保存在: {output_dir}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
