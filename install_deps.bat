@echo off
chcp 65001 >nul
setlocal EnableDelayedExpansion

echo ============================================================
echo  N.I.N.A. AI Cortex - Dependency Installer
echo ============================================================
echo.

:: Переход в директорию скрипта
cd /d "%~dp0"

:: Проверка Python
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Python not found!
    echo Please install Python 3.11+ from python.org
    echo Make sure to check "Add Python to PATH" during installation.
    pause
    exit /b 1
)

:: Показать версию Python
echo [OK] Python found:
python --version
echo.

:: Создание виртуального окружения
echo [1/4] Creating virtual environment...
if not exist "venv" (
    python -m venv venv
    if !errorlevel! neq 0 (
        echo [ERROR] Failed to create virtual environment
        pause
        exit /b 1
    )
    echo [OK] Virtual environment created
) else (
    echo [OK] Virtual environment already exists
)

:: Активация и установка зависимостей
echo.
echo [2/4] Installing backend dependencies...
call venv\Scripts\activate.bat

python -m pip install --upgrade pip >nul 2>&1
pip install -r backend\requirements.txt

if !errorlevel! neq 0 (
    echo [ERROR] Failed to install dependencies
    pause
    exit /b 1
)

echo [OK] Dependencies installed successfully

:: Создание необходимых директорий
echo.
echo [3/4] Creating working directories...
if not exist "data" mkdir data
if not exist "data\vector_db" mkdir data\vector_db
if not exist "logs" mkdir logs
echo [OK] Directories created

:: Создание шаблона .env
echo.
echo [4/4] Creating configuration templates...
if not exist "backend\.env" (
    (
        echo INFLUXDB_TOKEN=my-super-secret-token
        echo HA_TOKEN=
        echo VAULT_MASTER_PASSWORD=dev-master-password-change-me
    ) > backend\.env
    echo [OK] backend\.env template created
) else (
    echo [OK] backend\.env already exists
)

echo.
echo ============================================================
echo  Installation completed successfully!
echo ============================================================
echo.
echo  Next steps:
echo  1. Edit config\settings.yaml with your N.I.N.A. paths
echo  2. Install Ollama from https://ollama.ai/download
echo  3. Run: ollama pull qwen2.5:14b
echo  4. Start Docker Desktop (for Qdrant and InfluxDB)
echo  5. Run start_cortex.bat
echo.
pause