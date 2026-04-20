#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试连接和端口
"""
import socket
import ssl

def test_port(host, port):
    print(f"Testing connection to {host}:{port}")

    # Test TCP connection
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, port))
        if result == 0:
            print(f"[OK] Port {port} is open")
            sock.close()
            return True
        else:
            print(f"[FAIL] Port {port} connection failed (error code: {result})")
            sock.close()
            return False
    except Exception as e:
        print(f"[ERROR] Connection error: {e}")
        return False

if __name__ == "__main__":
    host = "117.50.193.120"
    port = 8282

    print("=" * 50)
    test_port(host, port)
    print("=" * 50)

    # Try HTTPS connection
    try:
        import requests
        url = f"https://{host}:{port}"
        print(f"\nTrying HTTPS connection to: {url}")
        try:
            response = requests.get(url, verify=False, timeout=5)
            print(f"[OK] HTTPS connection successful, status code: {response.status_code}")
        except requests.exceptions.SSLError:
            print("[WARN] HTTPS connection encountered SSL error (possibly using self-signed certificate)")
        except Exception as e:
            print(f"[FAIL] HTTPS connection failed: {e}")
    except ImportError:
        print("requests library not installed, skipping HTTPS test")
