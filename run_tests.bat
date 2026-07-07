@echo off
chcp 65001 >nul
setlocal EnableDelayedExpansion

echo ============================================================
echo  N.I.N.A. AI Cortex - Test Suite
echo ============================================================
echo.

:: Переход в директорию скрипта
cd /d "%~dp0"

:: Активация виртуального окружения
if not exist "venv\Scripts\activate.bat" (
    echo [ERROR] Virtual environment not found!
    echo Please run install_deps.bat first.
    pause
    exit /b 1
)

call venv\Scripts\activate.bat
cd backend

:: Проверка наличия pytest
pip show pytest >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] pytest not installed. Installing...
    pip install pytest pytest-asyncio pytest-cov pytest-mock
)

echo.
echo ============================================================
echo  Running Unit and Integration Tests...
echo ============================================================
pytest tests/unit tests/integration --cov=app --cov-report=term-missing -v

echo.
echo ============================================================
echo  Running E2E Tests (Simulation Mode)...
echo ============================================================
pytest tests/e2e -v

echo.
echo ============================================================
echo  Tests completed
echo ============================================================
pause