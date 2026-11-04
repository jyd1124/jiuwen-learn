@echo off
REM ========================================
REM Agent Runtime Manager - Rebuild Script
REM ========================================
REM 彻底清除缓存并重新编译 CLI 和 Server，解决 editable install 缓存问题

set SCRIPT_DIR=%~dp0
set PROJECT_ROOT=%SCRIPT_DIR%..

echo ========================================
echo 开始清理缓存...
echo ========================================

REM 1. 清除 CLI venv 中的缓存
echo [1/7] 清除 CLI venv 缓存...
cd "%PROJECT_ROOT%\cli"
if exist ".venv\Lib\site-packages\openjiuwen_runtime" (
    rd /s /q ".venv\Lib\site-packages\openjiuwen_runtime"
    echo   - 已删除 CLI venv 中的 openjiuwen_runtime
)
for /d /r .venv %%d in (__pycache__) do @if exist "%%d" (
    rd /s /q "%%d" 2>nul
)
for /r .venv %%f in (*.pyc) do @if exist "%%f" (
    del /f /q "%%f" 2>nul
)

REM 2. 清除 Server venv 中的缓存
echo [2/7] 清除 Server venv 缓存...
cd "%PROJECT_ROOT%\server"
if exist ".venv\Lib\site-packages\openjiuwen_runtime" (
    rd /s /q ".venv\Lib\site-packages\openjiuwen_runtime"
    echo   - 已删除 Server venv 中的 openjiuwen_runtime
)
for /d /r .venv %%d in (__pycache__) do @if exist "%%d" (
    rd /s /q "%%d" 2>nul
)
for /r .venv %%f in (*.pyc) do @if exist "%%f" (
    del /f /q "%%f" 2>nul
)

REM 3. 清除 SDK 源码中的 pyc 缓存
echo [3/7] 清除 SDK 源码缓存...
cd "%PROJECT_ROOT%\sdk"
for /d /r %%d in (__pycache__) do @if exist "%%d" (
    rd /s /q "%%d" 2>nul
)
for /r %%f in (*.pyc) do @if exist "%%f" (
    del /f /q "%%f" 2>nul
)

REM 4. 卸载旧版本 SDK (CLI)
echo [4/7] 卸载 CLI 旧版本包...
cd "%PROJECT_ROOT%\cli"
call uv pip uninstall openjiuwen-runtime-management-sdk 2>nul
call uv pip uninstall agent-runtime-manager-cli 2>nul

REM 5. 重新安装 SDK (CLI)
echo [5/7] 安装 SDK 到 CLI (editable mode)...
call uv pip install -e "%PROJECT_ROOT%\sdk"

REM 6. 重新安装 CLI
echo [6/7] 安装 CLI...
call uv pip install -e "./"

REM 7. 重新安装 SDK (Server) - 使用 --python 指定Server的venv
echo [7/7] 安装 SDK 到 Server (editable mode)...
cd "%PROJECT_ROOT%\server"
if exist ".venv\Scripts\python.exe" (
    call uv pip install --python .venv\Scripts\python.exe -e "%PROJECT_ROOT%\sdk"
) else (
    echo   ! Server venv 不存在，跳过
)

echo ========================================
echo 编译完成!
echo ========================================
echo.
echo 验证 CLI 安装:
cd "%PROJECT_ROOT%\cli"
.venv\Scripts\python.exe -c "from openjiuwen_runtime.management.sdk import DeploymentManager; import inspect; print('SDK OK:', inspect.signature(DeploymentManager.deploy_agent))" 2>nul
if errorlevel 1 (
    echo   ! CLI 验证失败
)
echo.
echo 验证 Server 安装:
cd "%PROJECT_ROOT%\server"
if exist ".venv\Scripts\python.exe" (
    .venv\Scripts\python.exe -c "from openjiuwen_runtime.management.sdk import DeploymentManager; import inspect; src=inspect.getsource(DeploymentManager.deploy_agent); print('SDK OK') if '\"name\"' in src and 'return' in src else print('SDK FAIL: missing name in return')" 2>nul
    if errorlevel 1 (
        echo   ! Server 验证失败
    )
) else (
    echo   ! Server venv 不存在，跳过验证
)
echo.
echo ========================================
echo 注意: 如果 Server 正在运行，请重启 Server 以加载新代码
echo ========================================
pause