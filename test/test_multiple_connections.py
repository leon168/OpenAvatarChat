#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试多种连接方式
"""
import asyncio
import json
import websockets


async def test_connection(uri, description):
    print(f"\n{'='*60}")
    print(f"Testing: {description}")
    print(f"URI: {uri}")
    print(f"{'='*60}")

    try:
        async with websockets.connect(uri, ssl=None) as ws:
            print(f"[OK] Connected successfully!")

            # Wait for READY
            try:
                response = await asyncio.wait_for(ws.recv(), timeout=5)
                data = json.loads(response)
                print(f"[OK] Received: {data}")

                if data.get("type") == "READY":
                    return True
                else:
                    print(f"[WARN] Expected READY, got: {data.get('type')}")
                    return False
            except asyncio.TimeoutError:
                print("[FAIL] Timeout waiting for READY message")
                return False

    except Exception as e:
        print(f"[FAIL] Connection failed: {e}")
        return False


async def main():
    host = "localhost"  # Change to "117.50.193.120" for remote
    port = 8282

    test_cases = [
        {
            "uri": f"ws://{host}:{port}/ws/xiling?appId=test&token=test&liveRoom=test",
            "desc": "WebSocket (ws://)"
        },
        {
            "uri": f"ws://{host}:{port}/ws/xiling",
            "desc": "WebSocket without parameters"
        },
        {
            "uri": f"ws://{host}:{port}/",
            "desc": "Root path"
        }
    ]

    results = []
    for test in test_cases:
        success = await test_connection(test["uri"], test["desc"])
        results.append((test["desc"], success))

    print(f"\n{'='*60}")
    print("Summary:")
    print(f"{'='*60}")
    for desc, success in results:
        status = "[OK]" if success else "[FAIL]"
        print(f"{status} {desc}")

    if not any(success for _, success in results):
        print("\nAll connections failed. Please check:")
        print("1. Is the server running?")
        print("2. Is the port correct?")
        print("3. Check server logs for errors")


if __name__ == "__main__":
    asyncio.run(main())
