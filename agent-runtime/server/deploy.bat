@echo off
setlocal
REM Agent Runtime Server one-click deploy script (Windows)

echo ========================================
echo   Agent Runtime Server Deploy Script
echo ========================================
echo.

REM 1) Check uv
echo [1/5] Checking environment...
uv --version >nul 2>&1
if %errorlevel% neq 0 (
    echo   [ERROR] uv is not installed. Please run: pip install uv
    exit /b 1
)
echo   [OK] uv is installed
uv --version

REM Move to script directory
cd /d "%~dp0"
echo   Working directory: %CD%

REM Find an available port starting from 8100 (do not kill existing process)
echo.
echo [pre] Finding available port from :8100...
set "SERVER_PORT="
for /f %%p in ('powershell -NoProfile -Command "$start=8100; for($i=$start; $i -lt $start+200; $i++){ $used=Get-NetTCPConnection -LocalPort $i -State Listen -ErrorAction SilentlyContinue; if(-not $used){ Write-Output $i; exit 0 } }; exit 1"') do set "SERVER_PORT=%%p"
if "%SERVER_PORT%"=="" (
    echo   [ERROR] No available port found in range 8100-8299
    exit /b 1
)
echo   [OK] Selected port: %SERVER_PORT%

REM 2) Create venv
echo.
echo [2/5] Creating virtual environment...
if not exist ".venv" (
    uv venv
    echo   [OK] Virtual environment created
) else (
    if exist ".venv\pyvenv.cfg" (
        echo   [OK] Virtual environment already exists
    ) else (
        echo   [WARN] .venv is incomplete ^(missing pyvenv.cfg^), recreating...
        if exist ".venv" rmdir /s /q ".venv"
        uv venv
        if errorlevel 1 (
            echo   [ERROR] Failed to recreate virtual environment
            echo   [HINT] Close running servers/terminals that may lock .venv, then retry
            exit /b 1
        )
        echo   [OK] Virtual environment recreated
    )
)

REM 3) Sync dependencies if pyproject exists
echo.
echo [3/5] Sync dependencies...
uv sync >nul 2>&1
if %errorlevel% neq 0 (
    echo   [WARN] pyproject.toml not found, skip uv sync
) else (
    echo   [OK] Dependencies synced
)

REM 4) Install management SDK
echo.
echo [4/5] Installing management SDK...
uv pip install -e ..\management --force-reinstall >nul 2>&1
if errorlevel 1 (
    echo   [ERROR] Failed to install management SDK
    echo   [HINT] Ensure no old server process is using .venv files
    exit /b 1
)
echo   [OK] management SDK installed

REM 5) Install foundation SDK
echo.
echo [5/5] Installing foundation SDK...
uv pip install -e ..\foundation --force-reinstall >nul 2>&1
if errorlevel 1 (
    echo   [ERROR] Failed to install foundation SDK
    echo   [HINT] Ensure no old server process is using .venv files
    exit /b 1
)
echo   [OK] foundation SDK installed

REM Start server
echo.
echo Starting server...
echo.
echo ========================================
echo   API:  http://localhost:%SERVER_PORT%
echo   Docs: http://localhost:%SERVER_PORT%/docs
echo ========================================
echo.

uv run -m uvicorn openjiuwen_runtime.server.main:app --host 0.0.0.0 --port %SERVER_PORT%
