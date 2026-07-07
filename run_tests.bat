@echo off
echo ============================================================
echo  N.I.N.A. AI Cortex - Test Suite
echo ============================================================
echo.

call venv\Scripts\activate.bat
cd backend

echo Запуск Unit и Integration тестов...
pytest tests/unit tests/integration --cov=app --cov-report=term-missing

echo.
echo Запуск E2E тестов (Simulation Mode)...
pytest tests/e2e -v

pause