"""
测试脚本：验证 SRT 推流 Handler

使用方法:
1. 启动 SRS: docker run -d --name srs -p 1935:1935 -p 8080:8080 -p 10080:10080/udp ossrs/srs:5
2. 运行测试: python test/test_srt_output.py
3. OBS 拉流: rtmp://localhost/live/avatar

功能:
- 测试 SRT 连接
- 测试音视频推流
"""

import asyncio
import os
import sys
import time
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.handlers.output.srt.srt_handler import SRTOutputHandler, SRTOutputConfig


async def test_srt_output():
    """测试 SRT 推流"""
    print("=" * 60)
    print("SRT 推流 Handler 测试")
    print("=" * 60)
    
    # 配置
    config = SRTOutputConfig(
        srt_url="srt://127.0.0.1:10080?streamid=#!::r=live/avatar,m=publish",
        width=512,
        height=512,
        fps=25,
        video_bitrate=2000,
        audio_bitrate=128,
        latency_ms=120
    )
    
    print(f"\n[+] 配置:")
    print(f"    SRT URL: {config.srt_url}")
    print(f"    分辨率: {config.width}x{config.height}")
    print(f"    帧率: {config.fps} FPS")
    print(f"    视频码率: {config.video_bitrate} kbps")
    print(f"    音频码率: {config.audio_bitrate} kbps")
    print(f"    SRT 延迟: {config.latency_ms} ms")
    
    # 创建 handler
    handler = SRTOutputHandler()
    handler.load(None, config)
    
    # 启动 ffmpeg
    print("\n[*] 启动 ffmpeg SRT 推流...")
    try:
        handler._start_ffmpeg()
        print("[+] ffmpeg 启动成功")
    except Exception as e:
        print(f"[!] ffmpeg 启动失败: {e}")
        return
    
    # 模拟推流 10 秒
    print("\n[*] 开始推流 (10 秒)...")
    print("    请用 OBS 或 ffplay 拉流:")
    print("    RTMP: rtmp://localhost/live/avatar")
    print("    SRT:  srt://localhost:10080?streamid=#!::r=live/avatar,m=request")
    print()
    
    frame_count = 0
    start_time = time.time()
    
    try:
        while time.time() - start_time < 10:
            # 生成测试视频帧 (渐变颜色)
            t = time.time() - start_time
            frame = np.zeros((512, 512, 3), dtype=np.uint8)
            frame[:, :, 0] = int(128 + 127 * np.sin(t * 2))  # R
            frame[:, :, 1] = int(128 + 127 * np.sin(t * 3))  # G
            frame[:, :, 2] = int(128 + 127 * np.sin(t * 5))  # B
            
            # 添加文字
            from PIL import Image, ImageDraw, ImageFont
            try:
                pil_img = Image.fromarray(frame)
                draw = ImageDraw.Draw(pil_img)
                text = f"SRT Test {t:.1f}s"
                draw.text((10, 10), text, fill=(255, 255, 255))
                frame = np.array(pil_img)
            except ImportError:
                pass
            
            # 生成测试音频 (1kHz 正弦波)
            samples_per_frame = 16000 // 25  # 16000Hz / 25fps
            audio = np.sin(2 * np.pi * 1000 * np.arange(samples_per_frame) / 16000)
            audio = (audio * 32767).astype(np.int16)
            
            # 写入帧
            handler._write_frame(frame, audio)
            
            frame_count += 1
            if frame_count % 25 == 0:
                print(f"    已推送 {frame_count} 帧 ({frame_count/25:.1f}s)", end='\r')
            
            # 控制帧率
            await asyncio.sleep(1/25)
            
    except KeyboardInterrupt:
        print("\n[!] 用户中断")
    finally:
        # 停止推流
        print("\n\n[*] 停止推流...")
        handler._stop_ffmpeg()
        print("[+] 推流结束")
        
        elapsed = time.time() - start_time
        print(f"\n[+] 统计:")
        print(f"    总帧数: {frame_count}")
        print(f"    时长: {elapsed:.2f}s")
        print(f"    实际帧率: {frame_count/elapsed:.1f} FPS")


if __name__ == "__main__":
    asyncio.run(test_srt_output())
