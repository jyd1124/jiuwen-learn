#!/bin/bash
# ========================================
# Agent Runtime Manager - Rebuild Script
# ========================================
# 彻底清除缓存并重新编译 CLI 和 Server，解决 editable install 缓存问题

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR/.."

echo "========================================"
echo "开始清理缓存..."
echo "========================================"

# 1. 清除 CLI venv 中的缓存
echo "[1/7] 清除 CLI venv 缓存..."
cd "$PROJECT_ROOT/cli"
if [ -d ".venv/lib/python*/site-packages/openjiuwen_runtime" ]; then
    rm -rf .venv/lib/python*/site-packages/openjiuwen_runtime
    echo "  - 已删除 CLI venv 中的 openjiuwen_runtime"
fi
find .venv -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find .venv -type f -name "*.pyc" -delete 2>/dev/null || true

# 2. 清除 Server venv 中的缓存
echo "[2/7] 清除 Server venv 缓存..."
cd "$PROJECT_ROOT/server"
if [ -d ".venv/lib/python*/site-packages/openjiuwen_runtime" ]; then
    rm -rf .venv/lib/python*/site-packages/openjiuwen_runtime
    echo "  - 已删除 Server venv 中的 openjiuwen_runtime"
fi
find .venv -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find .venv -type f -name "*.pyc" -delete 2>/dev/null || true

# 3. 清除 SDK 源码中的 pyc 缓存
echo "[3/7] 清除 SDK 源码缓存..."
cd "$PROJECT_ROOT/sdk"
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find . -type f -name "*.pyc" -delete 2>/dev/null || true

# 4. 卸载旧版本 SDK (CLI)
echo "[4/7] 卸载 CLI 旧版本包..."
cd "$PROJECT_ROOT/cli"
uv pip uninstall openjiuwen-runtime-management-sdk 2>/dev/null || true
uv pip uninstall agent-runtime-manager-cli 2>/dev/null || true

# 5. 重新安装 SDK (CLI)
echo "[5/7] 安装 SDK 到 CLI (editable mode)..."
uv pip install -e "$PROJECT_ROOT/sdk"

# 6. 重新安装 CLI
echo "[6/7] 安装 CLI..."
uv pip install -e ./

# 7. 重新安装 SDK (Server) - 使用 --python 指定Server的venv
echo "[7/7] 安装 SDK 到 Server (editable mode)..."
cd "$PROJECT_ROOT/server"
if [ -f ".venv/bin/python" ]; then
    uv pip install --python .venv/bin/python -e "$PROJECT_ROOT/sdk"
else
    echo "  ! Server venv 不存在，跳过"
fi

echo "========================================"
echo "编译完成!"
echo "========================================"
echo ""
echo "验证 CLI 安装:"
cd "$PROJECT_ROOT/cli"
if .venv/bin/python -c "from openjiuwen_runtime.management.sdk import DeploymentManager; import inspect; print('SDK OK:', inspect.signature(DeploymentManager.deploy_agent))" 2>/dev/null; then
    :
else
    echo "  ! CLI 验证失败"
fi

echo ""
echo "验证 Server 安装:"
cd "$PROJECT_ROOT/server"
if [ -f ".venv/bin/python" ]; then
    if .venv/bin/python -c "
from openjiuwen_runtime.management.sdk import DeploymentManager
import inspect
src = inspect.getsource(DeploymentManager.deploy_agent)
print('SDK OK') if '\"name\"' in src and 'return' in src else print('SDK FAIL: missing name in return')
" 2>/dev/null; then
        :
    else
        echo "  ! Server 验证失败"
    fi
else
    echo "  ! Server venv 不存在，跳过验证"
fi

echo ""
echo "========================================"
echo "注意: 如果 Server 正在运行，请重启 Server 以加载新代码"
echo "========================================"