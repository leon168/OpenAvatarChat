import re

# 读取文件
with open('src/handlers/tts/xiling_tts/tts_handler_xiling.py', 'r', encoding='utf-8') as f:
    content = f.read()

# 修复正则表达式
old = 'text = re.sub(r""""<\\|.*?\\|>""", "", text)'
new = "text = re.sub(r'<\\|.*?\\|>', '', text)"

if old in content:
    content = content.replace(old, new)
    with open('src/handlers/tts/xiling_tts/tts_handler_xiling.py', 'w', encoding='utf-8') as f:
        f.write(content)
    print('Fixed!')
else:
    print('Pattern not found, checking...')
    # 查找包含 re.sub 的行
    for i, line in enumerate(content.split('\n'), 1):
        if 're.sub' in line and 'SSML' in content.split('\n')[i-2] if i > 1 else False:
            print(f'Line {i}: {repr(line)}')
