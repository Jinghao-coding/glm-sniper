#!/bin/bash
set -e
cd "$(dirname "$0")"

if [ ! -f .env ]; then
    echo "未找到 .env，正在从 .env.example 创建..."
    cp .env.example .env
    echo "请编辑 .env 填入 COOKIE_STRING 和 PREVIEW_BODY 后重新运行"
    exit 1
fi

if [ ! -d .venv ]; then
    echo "创建虚拟环境..."
    uv venv
fi

source .venv/bin/activate

if ! python -c "import aiohttp" 2>/dev/null; then
    echo "安装依赖..."
    uv pip install -r requirements.txt
fi

exec python glm_sniper.py
