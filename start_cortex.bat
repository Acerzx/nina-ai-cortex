@echo off
chcp 65001 >nul
setlocal EnableDelayedExpansion
title N.I.N.A. AI Cortex - Backend Server

echo ============================================================
echo  N.I.N.A. AI Cortex - Backend Server
echo ============================================================
echo.

:: Переход в директорию скрипта
cd /d "%~dp0"

:: Проверка виртуального окружения
if not exist "venv\Scripts\activate.bat" (
    echo [ERROR] Virtual environment not found!
    echo Please run install_deps.bat first.
    pause
    exit /b 1
)

:: Активация
call venv\Scripts\activate.bat

:: Создание .env файла если отсутствует
if not exist "backend\.env" (
    echo [WARNING] backend\.env not found. Creating template...
    (
        echo INFLUXDB_TOKEN=my-super-secret-token
        echo HA_TOKEN=
        echo VAULT_MASTER_PASSWORD=dev-master-password-change-me
    ) > backend\.env
    echo [OK] Template created at backend\.env
)

:: Проверка Docker (исправленная версия без проблемных операторов)
echo Checking Docker...
set DOCKER_AVAILABLE=0

where docker >nul 2>&1
if %errorlevel% equ 0 (
    set DOCKER_AVAILABLE=1
    echo [OK] Docker found.
)

if !DOCKER_AVAILABLE! equ 1 (
    echo Checking if Docker Desktop is running...
    docker info >nul 2>&1
    set DOCKER_RUNNING=!errorlevel!
    
    if !DOCKER_RUNNING! equ 0 (
        echo [OK] Docker is running. Starting infrastructure...
        
        :: Пробуем новый синтаксис docker compose
        docker compose up -d >nul 2>&1
        set COMPOSE_RESULT=!errorlevel!
        
        :: Если не сработало, пробуем старый синтаксис
        if !COMPOSE_RESULT! neq 0 (
            docker-compose up -d >nul 2>&1
            set COMPOSE_RESULT=!errorlevel!
        )
        
        if !COMPOSE_RESULT! equ 0 (
            echo [OK] Infrastructure started successfully
        ) else (
            echo [WARNING] Failed to start containers. Check Docker Desktop.
        )
    ) else (
        echo [WARNING] Docker Desktop is not running.
        echo Please start Docker Desktop or ensure Qdrant and InfluxDB are available.
    )
) else (
    echo [WARNING] Docker is not installed.
    echo Make sure Qdrant ^(port 6333^) and InfluxDB ^(port 8086^) are running.
)

echo.
echo ============================================================
echo  Starting FastAPI server...
echo  API Docs: http://localhost:8000/docs
echo  WebSocket: ws://localhost:8000/ws
echo ============================================================
echo.

:: Переход в backend директорию
cd backend

:: Запуск uvicorn
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

pause