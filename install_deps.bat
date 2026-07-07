@echo off
echo ============================================================
echo  N.I.N.A. AI Cortex - Dependency Installer
echo ============================================================
echo.

:: Проверка Python
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Python не найден! Установите Python 3.11+ с python.org
    pause
    exit /b 1
)

:: Создание виртуального окружения
echo [1/3] Создание виртуального окружения...
if not exist "venv" (
    python -m venv venv
) else (
    echo Виртуальное окружение уже существует.
)

:: Активация и установка зависимостей
echo [2/3] Установка зависимостей Backend...
call venv\Scripts\activate.bat
pip install --upgrade pip
pip install -r backend\requirements.txt

:: Создание необходимых директорий
echo [3/3] Создание рабочих директорий...
if not exist "data" mkdir data
if not exist "data\vector_db" mkdir data\vector_db
if not exist "logs" mkdir logs

echo.
echo ============================================================
echo  Установка завершена успешно!
echo  Следующий шаг: Запустите start_cortex.bat
echo ============================================================
pause