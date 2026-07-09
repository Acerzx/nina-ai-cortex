"""
Thread Pool Executors — единый пул потоков для выполнения blocking I/O операций
в async контексте без блокировки event loop.

Решение проблем аудита:
- #11: shutil.rmtree/disk_usage в disk_monitor.py
- #12: shutil.disk_usage
- #13: CSV parsing в hocus_focus_watcher.py
- #14: JSON parsing в session_watcher.py
- #15: fits.getheader в fits_header_scanner.py

Архитектура:
- I/O-bound executor (8 workers) — файловые операции, DB
- CPU-bound executor (2 workers) — FITS processing, парсинг
- Async wrappers для всех blocking operations
"""

import asyncio
import logging
import shutil
from pathlib import Path
from typing import Any, Callable, Optional, List, Dict
from concurrent.futures import ThreadPoolExecutor
from functools import partial

logger = logging.getLogger("Executors")


# ============================================================================
# THREAD POOLS
# ============================================================================
# I/O-bound operations: файловые операции, HTTP, DB
_io_executor: Optional[ThreadPoolExecutor] = None
# CPU-bound operations: FITS parsing, image processing
_cpu_executor: Optional[ThreadPoolExecutor] = None

# Максимальное количество workers
_IO_MAX_WORKERS = 8
_CPU_MAX_WORKERS = 2


def get_io_executor() -> ThreadPoolExecutor:
    """Возвращает executor для I/O-bound операций."""
    global _io_executor
    if _io_executor is None:
        _io_executor = ThreadPoolExecutor(
            max_workers=_IO_MAX_WORKERS,
            thread_name_prefix="io-worker",
        )
        logger.info(f"✅ I/O executor created ({_IO_MAX_WORKERS} workers)")
    return _io_executor


def get_cpu_executor() -> ThreadPoolExecutor:
    """Возвращает executor для CPU-bound операций."""
    global _cpu_executor
    if _cpu_executor is None:
        _cpu_executor = ThreadPoolExecutor(
            max_workers=_CPU_MAX_WORKERS,
            thread_name_prefix="cpu-worker",
        )
        logger.info(f"✅ CPU executor created ({_CPU_MAX_WORKERS} workers)")
    return _cpu_executor


async def run_io(func: Callable, *args, **kwargs) -> Any:
    """
    Выполняет I/O-bound blocking функцию в executor.

    Args:
        func: Blocking функция
        *args, **kwargs: Аргументы функции

    Returns:
        Результат выполнения функции
    """
    loop = asyncio.get_running_loop()
    executor = get_io_executor()

    if kwargs:
        func = partial(func, **kwargs)

    return await loop.run_in_executor(executor, func, *args)


async def run_cpu(func: Callable, *args, **kwargs) -> Any:
    """
    Выполняет CPU-bound blocking функцию в executor.

    Args:
        func: Blocking функция
        *args, **kwargs: Аргументы функции

    Returns:
        Результат выполнения функции
    """
    loop = asyncio.get_running_loop()
    executor = get_cpu_executor()

    if kwargs:
        func = partial(func, **kwargs)

    return await loop.run_in_executor(executor, func, *args)


# ============================================================================
# ASYNC WRAPPERS ДЛЯ ЧАСТЫХ ОПЕРАЦИЙ
# ============================================================================
async def async_rmtree(path: Path) -> bool:
    """
    Асинхронное удаление директории с поддиректориями.

    Args:
        path: Путь к директории

    Returns:
        True если успешно удалено
    """
    try:
        await run_io(shutil.rmtree, str(path))
        logger.debug(f"✅ Removed directory: {path}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to remove {path}: {e}")
        return False


async def async_disk_usage(path: Path) -> Optional[Dict[str, float]]:
    """
    Асинхронное получение информации об использовании диска.

    Args:
        path: Путь для проверки

    Returns:
        Словарь {total_gb, used_gb, free_gb} или None при ошибке
    """
    try:
        total, used, free = await run_io(shutil.disk_usage, str(path))
        return {
            "total_gb": total / (1024**3),
            "used_gb": used / (1024**3),
            "free_gb": free / (1024**3),
        }
    except Exception as e:
        logger.error(f"❌ Failed to get disk usage for {path}: {e}")
        return None


async def async_read_json(path: Path) -> Optional[Dict]:
    """
    Асинхронное чтение JSON файла.

    Args:
        path: Путь к JSON файлу

    Returns:
        Распарсенный JSON или None при ошибке
    """
    import json

    try:

        def _read():
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)

        return await run_io(_read)
    except Exception as e:
        logger.error(f"❌ Failed to read JSON {path}: {e}")
        return None


async def async_read_csv(path: Path, delimiter: str = None) -> Optional[List[Dict]]:
    """
    Асинхронное чтение CSV файла.

    Args:
        path: Путь к CSV файлу
        delimiter: Разделитель (auto-detect если None)

    Returns:
        Список словарей (rows) или None при ошибке
    """
    import csv

    try:

        def _read():
            with open(path, "r", encoding="utf-8") as f:
                # Auto-detect delimiter
                if delimiter is None:
                    sample = f.readline()
                    f.seek(0)
                    _delimiter = ";" if ";" in sample else ","
                else:
                    _delimiter = delimiter

                reader = csv.DictReader(f, delimiter=_delimiter)
                return list(reader)

        return await run_io(_read)
    except Exception as e:
        logger.error(f"❌ Failed to read CSV {path}: {e}")
        return None


async def async_fits_getheader(path: Path, ext: int = 0) -> Optional[Dict]:
    """
    Асинхронное чтение FITS заголовков.

    Args:
        path: Путь к FITS файлу
        ext: Extension index (default: 0)

    Returns:
        Словарь заголовков или None при ошибке
    """
    try:
        from astropy.io import fits

        def _read():
            header = fits.getheader(str(path), ext=ext)
            return dict(header)

        return await run_cpu(_read)
    except Exception as e:
        logger.error(f"❌ Failed to read FITS header {path}: {e}")
        return None


async def shutdown_executors():
    """
    Корректно закрывает все thread pool executors.
    Вызывается при shutdown приложения.
    """
    global _io_executor, _cpu_executor

    logger.info("🛑 Shutting down executors...")

    if _io_executor:
        _io_executor.shutdown(wait=True)
        _io_executor = None
        logger.info("   ✅ I/O executor shutdown")

    if _cpu_executor:
        _cpu_executor.shutdown(wait=True)
        _cpu_executor = None
        logger.info("   ✅ CPU executor shutdown")
