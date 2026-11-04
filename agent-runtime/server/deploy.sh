#!/bin/bash
# Agent Runtime Server one-click deploy script

set -e

echo "========================================"
echo "  Agent Runtime Server Deploy Script"
echo "========================================"
echo ""

# Check uv
echo "[1/5] Checking environment..."
if ! command -v uv &> /dev/null; then
    echo "  [ERROR] uv is not installed. Please install it first: pip install uv"
    exit 1
fi
UV_VERSION=$(uv --version)
echo "  ✓ uv $UV_VERSION"

# Switch to script directory
cd "$(dirname "$0")"
echo "  Working directory: $(pwd)"

# Find an available port starting from 8100
echo ""
echo "[pre] Checking available port (starting from 8100)..."
SERVER_PORT=""
for p in $(seq 8100 8299); do
    if command -v ss >/dev/null 2>&1; then
        if ss -ltn "( sport = :$p )" 2>/dev/null | awk 'NR>1{found=1} END{exit found?0:1}'; then
            continue
        fi
        SERVER_PORT="$p"
        break
    elif command -v lsof >/dev/null 2>&1; then
        if lsof -iTCP:"$p" -sTCP:LISTEN -t >/dev/null 2>&1; then
            continue
        fi
        SERVER_PORT="$p"
        break
    else
        echo "  [ERROR] Missing port-check tools: please install ss (iproute2) or lsof"
        exit 1
    fi
done
if [ -z "${SERVER_PORT}" ]; then
    echo "  [ERROR] No available port found in range 8100-8299"
    exit 1
fi
echo "  [OK] Selected port: $SERVER_PORT"

# Create virtual environment
echo ""
echo "[2/5] Creating virtual environment..."
if [ ! -d ".venv" ]; then
    uv venv
    echo "  [OK] Virtual environment created"
else
    echo "  [OK] Virtual environment already exists"
fi

# Sync dependencies
echo ""
echo "[3/5] Syncing dependencies..."
if [ -f "pyproject.toml" ]; then
    uv sync > /dev/null 2>&1
    echo "  [OK] Dependencies synced"
else
    echo "  [WARN] No pyproject.toml, skipping uv sync"
fi

# Install management SDK
echo ""
echo "[4/5] Installing management SDK..."
uv pip install -e ../management --force-reinstall > /dev/null 2>&1
echo "  [OK] management SDK installed"

# Install foundation SDK
echo ""
echo "[5/5] Installing foundation SDK..."
uv pip install -e ../foundation --force-reinstall > /dev/null 2>&1
echo "  [OK] foundation SDK installed"

# Start server
echo ""
echo "Starting server..."
echo ""
echo "========================================"
echo "  API: http://localhost:$SERVER_PORT"
echo "  Docs: http://localhost:$SERVER_PORT/docs"
echo "========================================"
echo ""

uv run -m uvicorn openjiuwen_runtime.server.main:app --host 0.0.0.0 --port "$SERVER_PORT"
