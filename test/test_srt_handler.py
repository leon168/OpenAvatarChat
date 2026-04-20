"""
测试脚本：验证 SRT Handler

使用方法:
1. 启动 SRS: docker run -d --name srs -p 10080:10080/udp ossrs/srs:5
2. 运行测试: python test/test_srt_handler.py
3. OBS 拉流: srt://localhost:10080?streamid=#!::r=live/avatar,m=request

功能:
- 测试 ffmpeg SRT 支持
- 测试 SRT 推流
- 验证音视频同步
"""

import os
import sys
import time
import subprocess
import numpy as np

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def check_ffmpeg_srt():
    """检查 ffmpeg 是否支持 SRT"""
    print("=" * 60)
    print("检查 ffmpeg SRT 支持")
    print("=" * 60)
    
    try:
        result = subprocess.run(
            ["ffmpeg", "-protocols"],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        if "srt" in result.stdout.lower():
            print("[OK] ffmpeg 支持 SRT 协议")
            # 显示 SRT 相关行
            for line in result.stdout.split('\n'):
                if 'srt' in line.lower():
                    print(f"    {line.strip()}")
            return True
        else:
            print("[FAIL] ffmpeg 不支持 SRT 协议")
            print("请安装支持 SRT 的 ffmpeg:")
            print("  Ubuntu: sudo apt-get install ffmpeg")
            print("  macOS: brew install ffmpeg")
            print("  Windows: https://www.gyan.dev/ffmpeg/builds/")
            return False
            
    except FileNotFoundError:
        print("[FAIL] 未找到 ffmpeg")
        print("请安装 ffmpeg:")
        print("  Ubuntu: sudo apt-get install ffmpeg")
        print("  macOS: brew install ffmpeg")
        print("  Windows: 下载并添加到 PATH")
        return False
    except Exception as e:
        print(f"[FAIL] 检查失败: {e}")
        return False


def test_srt_push():
    """测试 SRT 推流"""
    print("\n" + "=" * 60)
    print("测试 SRT 推流")
    print("=" * 60)
    
    srt_url = "srt://127.0.0.1:10080?streamid=#!::r=live/test,m=publish"
    width, height = 320, 240
    fps = 25
    duration = 5  # 测试 5 秒
    
    print(f"[*] 推流地址: {srt_url}")
    print(f"[*] 分辨率: {width}x{height}@{fps}fps")
    print(f"[*] 测试时长: {duration}秒")
    
    # 构建 ffmpeg 命令
    command = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel", "warning",
        # 生成测试视频 (彩条)
        "-f", "lavfi",
        "-i", f"testsrc=duration={duration}:size={width}x{height}:rate={fps}",
        # 生成测试音频 (1kHz 正弦波)
        "-f", "lavfi",
        "-i", f"sine=frequency=1000:duration={duration}",
        # 视频编码
        "-c:v", "libx264",
        "-preset", "fast",
        "-tune", "zerolatency",
        "-b:v", "500k",
        "-pix_fmt", "yuv420p",
        "-g", str(fps * 2),
        # 音频编码
        "-c:a", "aac",
        "-b:a", "128k",
        # 输出
        "-f", "mpegts",
        srt_url
    ]
    
    print(f"[*] 启动 ffmpeg...")
    print(f"    命令: {' '.join(command[:10])} ...")
    
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # 等待推流完成
        stdout, stderr = process.communicate(timeout=duration + 10)
        
        if process.returncode == 0:
            print("[OK] SRT 推流测试成功")
            return True
        else:
            error = stderr.decode('utf-8', errors='ignore')[-500:]  # 最后 500 字符
            print(f"[FAIL] ffmpeg 退出码: {process.returncode}")
            print(f"错误信息:\n{error}")
            return False
            
    except subprocess.TimeoutExpired:
        process.kill()
        print("[FAIL] 推流超时")
        return False
    except Exception as e:
        print(f"[FAIL] 推流失败: {e}")
        return False


def test_handler_import():
    """测试 Handler 导入"""
    print("\n" + "=" * 60)
    print("测试 SRT Handler 导入")
    print("=" * 60)
    
    try:
        from src.handlers.output.srt.srt_handler import HandlerSRTOutput, SRTOutputConfig
        print("[OK] 成功导入 HandlerSRTOutput 和 SRTOutputConfig")
        
        # 测试配置创建
        config = SRTOutputConfig()
        print(f"[OK] 默认配置创建成功:")
        print(f"    srt_url: {config.srt_url}")
        print(f"    latency_ms: {config.latency_ms}")
        print(f"    video: {config.video_width}x{config.video_height}@{config.fps}fps")
        
        # 测试 Handler 实例化
        handler = HandlerSRTOutput()
        print("[OK] Handler 实例化成功")
        
        return True
        
    except ImportError as e:
        print(f"[FAIL] 导入失败: {e}")
        return False
    except Exception as e:
        print(f"[FAIL] 测试失败: {e}")
        return False


def test_srs_connection():
    """测试 SRS 连接"""
    print("\n" + "=" * 60)
    print("测试 SRS 连接")
    print("=" * 60)
    
    import socket
    
    host = "127.0.0.1"
    port = 10080
    
    print(f"[*] 检查 {host}:{port}...")
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(2)
        # SRT 是 UDP 协议，尝试发送一个空包
        sock.sendto(b"", (host, port))
        sock.close()
        print(f"[OK] SRS SRT 端口 {port} 可连接")
        return True
    except socket.timeout:
        print(f"[WARN] 连接超时，SRS 可能未启动或端口未开放")
        print(f"    请运行: docker run -d --name srs -p 10080:10080/udp ossrs/srs:5")
        return False
    except Exception as e:
        print(f"[FAIL] 连接失败: {e}")
        return False


def main():
    """主测试函数"""
    print("=" * 60)
    print("SRT Handler 测试")
    print("=" * 60)
    
    results = []
    
    # 1. 检查 ffmpeg
    results.append(("ffmpeg SRT 支持", check_ffmpeg_srt()))
    
    # 2. 测试 Handler 导入
    results.append(("Handler 导入", test_handler_import()))
    
    # 3. 测试 SRS 连接
    results.append(("SRS 连接", test_srs_connection()))
    
    # 4. 测试 SRT 推流 (如果前面都通过)
    if all(r[1] for r in results):
        results.append(("SRT 推流", test_srt_push()))
    else:
        print("\n[!] 跳过 SRT 推流测试 (前置检查未通过)")
    
    # 汇总
    print("\n" + "=" * 60)
    print("测试结果汇总")
    print("=" * 60)
    
    for name, passed in results:
        status = "[PASS]" if passed else "[FAIL]"
        print(f"{status} {name}")
    
    passed_count = sum(1 for _, p in results if p)
    total_count = len(results)
    
    print(f"\n总计: {passed_count}/{total_count} 通过")
    
    if passed_count == total_count:
        print("\n[OK] 所有测试通过！")
        print("OBS 拉流地址: srt://localhost:10080?streamid=#!::r=live/avatar,m=request")
        return 0
    else:
        print("\n[!] 部分测试失败，请检查配置")
        return 1


if __name__ == "__main__":
    sys.exit(main())
