#!/usr/bin/env python3
"""
测试百度千帆 LLM (Ernie-4.5-Turbo)
配置文件: config/chat_with_openai_compatible_xiling.yaml

使用 OpenAI Compatible API 调用百度千帆大模型
API 地址: https://qianfan.baidubce.com/v2
模型: ernie-4.5-turbo-20260402
"""
import asyncio
import argparse
import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import aiohttp


async def test_llm_api_key(api_key: str, model: str = "ernie-4.5-turbo-20260402"):
    """
    使用 API Key 调用百度千帆 LLM
    
    注意: 百度千帆需要使用 access_token 鉴权
    1. 先用 API Key + Secret Key 获取 access_token
    2. 再用 access_token 调用 LLM API
    """
    # 千帆 OpenAI Compatible API 地址
    api_url = "https://qianfan.baidubce.com/v2/chat/completions"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"  # 使用 access_token
    }
    
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "你是一个有帮助的AI助手，用简短的两三句话回答问题。"},
            {"role": "user", "content": "你好，请介绍一下自己"}
        ],
        "temperature": 0.7,
        "max_tokens": 512,
        "stream": False
    }
    
    print(f"调用模型: {model}")
    print(f"API 地址: {api_url}")
    print(f"请求内容: {json.dumps(payload, ensure_ascii=False, indent=2)}")
    print("-" * 50)
    
    async with aiohttp.ClientSession() as session:
        async with session.post(api_url, headers=headers, json=payload) as resp:
            result = await resp.json()
            print(f"状态码: {resp.status}")
            print(f"响应内容: {json.dumps(result, ensure_ascii=False, indent=2)}")
            
            if resp.status == 200:
                if "choices" in result and len(result["choices"]) > 0:
                    content = result["choices"][0]["message"]["content"]
                    print("-" * 50)
                    print(f"LLM 回复: {content}")
                    return True
            else:
                print(f"调用失败: {result.get('error', 'Unknown error')}")
                return False


async def test_llm_stream(api_key: str, model: str = "ernie-4.5-turbo-20260402"):
    """流式调用 LLM"""
    api_url = "https://qianfan.baidubce.com/v2/chat/completions"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }
    
    payload = {
        "model": model,
        "messages": [
            {"role": "user", "content": "讲一个简短的笑话"}
        ],
        "temperature": 0.7,
        "max_tokens": 512,
        "stream": True  # 启用流式输出
    }
    
    print(f"流式调用模型: {model}")
    print("-" * 50)
    
    async with aiohttp.ClientSession() as session:
        async with session.post(api_url, headers=headers, json=payload) as resp:
            print(f"状态码: {resp.status}")
            
            if resp.status != 200:
                result = await resp.json()
                print(f"调用失败: {result}")
                return False
            
            # 读取流式响应
            print("LLM 流式回复: ", end="", flush=True)
            async for line in resp.content:
                line = line.decode("utf-8").strip()
                if line.startswith("data: "):
                    data_str = line[6:]  # 去掉 "data: " 前缀
                    if data_str == "[DONE]":
                        break
                    try:
                        data = json.loads(data_str)
                        if "choices" in data and len(data["choices"]) > 0:
                            delta = data["choices"][0].get("delta", {})
                            content = delta.get("content", "")
                            if content:
                                print(content, end="", flush=True)
                    except json.JSONDecodeError:
                        pass
            print()  # 换行
            return True


def get_access_token(api_key: str, secret_key: str) -> str:
    """
    使用 API Key 和 Secret Key 获取百度 access_token
    
    文档: https://cloud.baidu.com/doc/WENXINWORKSHOP/s/Ilkkrb0i5
    """
    import requests
    
    url = "https://aip.baidubce.com/oauth/2.0/token"
    params = {
        "grant_type": "client_credentials",
        "client_id": api_key,
        "client_secret": secret_key
    }
    
    response = requests.post(url, params=params)
    result = response.json()
    
    if "access_token" in result:
        print(f"获取 access_token 成功，有效期: {result.get('expires_in', 'unknown')} 秒")
        return result["access_token"]
    else:
        raise ValueError(f"获取 access_token 失败: {result}")


async def main():
    parser = argparse.ArgumentParser(description="测试百度千帆 LLM")
    parser.add_argument("--api-key", help="API Key (用于获取 access_token)")
    parser.add_argument("--secret-key", help="Secret Key (用于获取 access_token)")
    parser.add_argument("--access-token", help="直接提供 access_token")
    parser.add_argument("--model", default="ernie-4.5-turbo-20260402", help="模型名称")
    parser.add_argument("--stream", action="store_true", help="使用流式输出")
    
    args = parser.parse_args()
    
    # 优先使用直接提供的 access_token
    if args.access_token:
        token = args.access_token
    elif args.api_key and args.secret_key:
        # 使用 API Key + Secret Key 获取 access_token
        token = get_access_token(args.api_key, args.secret_key)
    else:
        # 尝试从环境变量获取
        token = os.getenv("QIANFAN_ACCESS_TOKEN") or os.getenv("XILING_ACCESS_TOKEN")
        if not token:
            print("错误: 请提供 --access-token 或 --api-key + --secret-key")
            print("或设置环境变量 QIANFAN_ACCESS_TOKEN")
            return
    
    # 调用 LLM
    if args.stream:
        success = await test_llm_stream(token, args.model)
    else:
        success = await test_llm_api_key(token, args.model)
    
    if success:
        print("\n✓ 测试成功")
    else:
        print("\n✗ 测试失败")


if __name__ == "__main__":
    asyncio.run(main())
