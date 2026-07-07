@echo off
title N.I.N.A. AI Cortex - Backend Server
echo ============================================================
echo  Запуск N.I.N.A. AI Cortex Backend...
echo ============================================================
echo.

:: Проверка виртуального окружения
if not exist "venv\Scripts\activate.bat" (
    echo [ERROR] Виртуальное окружение не найдено!
    echo Запустите install_deps.bat сначала.
    pause
    exit /b 1
)

:: Активация
call venv\Scripts\activate.bat

:: Проверка .env файла
if not exist "backend\.env" (
    echo [WARNING] Файл backend\.env не найден. Создаю шаблон...
    echo INFLUXDB_TOKEN=my-super-secret-token > backend\.env
    echo HA_TOKEN= >> backend\.env
)

:: Запуск Docker контейнеров (если Docker Desktop запущен)
echo Проверка Docker...
docker info >nul 2>&1
if %errorlevel% equ 0 (
    echo Запуск инфраструктуры (Qdrant, InfluxDB)...
    docker-compose up -d
) else (
    echo [WARNING] Docker не запущен. Убедитесь, что Qdrant и InfluxDB доступны локально.
)

echo.
echo Запуск FastAPI сервера на http://localhost:8000
echo API Docs: http://localhost:8000/docs
echo.

:: Запуск uvicorn
cd backend
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

pause