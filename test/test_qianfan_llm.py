#!/usr/bin/env python3
"""
测试百度千帆 LLM (通过 OpenAI Compatible 接口)

使用配置:
- 模型: ernie-4.5-turbo-20260402
- API URL: https://qianfan.baidubce.com/v2
- 鉴权: DASHSCOPE_API_KEY 环境变量

运行方式 (PowerShell):
    $env:DASHSCOPE_API_KEY="your_api_key"
    uv run test/test_qianfan_llm.py
"""

import os
import sys

# 添加项目路径
project_dir = os.path.join(os.path.dirname(__file__), "..")
src_dir = os.path.join(project_dir, "src")
sys.path.insert(0, project_dir)
sys.path.insert(0, src_dir)

from openai import OpenAI


def test_qianfan_llm():
    """测试百度千帆 LLM"""
    
    # 检查 API Key
    api_key = os.getenv("DASHSCOPE_API_KEY")
    if not api_key:
        print("错误: 请设置环境变量 DASHSCOPE_API_KEY")
        print("示例: $env:DASHSCOPE_API_KEY='your_api_key'")
        sys.exit(1)
    
    print("=" * 60)
    print("百度千帆 LLM 测试")
    print("=" * 60)
    print(f"模型: ernie-4.5-turbo-20260402")
    print(f"API URL: https://qianfan.baidubce.com/v2")
    print(f"API Key: {api_key[:8]}...{api_key[-4:]}")
    print("=" * 60)
    
    # 创建 OpenAI 客户端
    client = OpenAI(
        api_key=api_key,
        base_url="https://qianfan.baidubce.com/v2",
        timeout=30.0,
    )
    
    # 系统提示词
    system_prompt = "请你扮演一个 AI 助手，用简短的两三句对话来回答用户的问题，并在对话内容中加入合适的标点符号，不需要讨论标点符号相关的内容"
    
    # 测试对话
    test_inputs = [
        "你好，请介绍一下自己",
        "今天天气怎么样？",
        "讲一个简短的笑话",
    ]
    
    for i, user_input in enumerate(test_inputs, 1):
        print(f"\n{'=' * 60}")
        print(f"测试 {i}/{len(test_inputs)}")
        print(f"用户: {user_input}")
        print(f"助手: ", end="", flush=True)
        
        try:
            # 调用 LLM
            completion = client.chat.completions.create(
                model="ernie-4.5-turbo-20260402",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_input},
                ],
                stream=True,
                stream_options={"include_usage": True}
            )
            
            # 接收流式输出
            for chunk in completion:
                if (chunk and chunk.choices and chunk.choices[0] 
                    and chunk.choices[0].delta.content):
                    content = chunk.choices[0].delta.content
                    print(content, end="", flush=True)
            
            print()  # 换行
            
        except Exception as e:
            print(f"\n错误: {e}")
            import traceback
            traceback.print_exc()
    
    print(f"\n{'=' * 60}")
    print("测试完成")
    print("=" * 60)


if __name__ == "__main__":
    test_qianfan_llm()
