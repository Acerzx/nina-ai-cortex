"""
Math Utilities — единый модуль статистических функций для Cortex.
Устраняет проблему С-4: дублирование формулы линейной регрессии
в 5 файлах проекта.

Функции:
- linear_regression(values) → (slope, intercept)
  Полная линейная регрессия методом наименьших квадратов.
  X — индексы [0, 1, 2, ..., n-1], Y — значения.

- calculate_trend(values) → float
  Возвращает только slope (наклон). Удобно для детекции трендов
  метрик (HFR, RMS, SNR, температура и т.д.).
  Положительное значение → рост (деградация для HFR, улучшение для SNR).
  Отрицательное значение → убывание.

- calculate_r_squared(values, slope, intercept) → float
  Коэффициент детерминации R² (0.0 — 1.0).
  Показывает, насколько хорошо линейная модель описывает данные.
  R² > 0.7 → хорошая линейная зависимость.

- pearson_correlation(x, y) → float
  Коэффициент корреляции Пирсона (-1.0 — 1.0).
  Используется для поиска взаимосвязей между метриками
  (например, температура ↔ HFR, ветер ↔ RMS).

Использование:
    from app.core.math_utils import (
        linear_regression,
        calculate_trend,
        calculate_r_squared,
        pearson_correlation,
    )

    # Тренд HFR (деградация?)
    hfr_trend = calculate_trend(hfr_history[-10:])
    if hfr_trend > 0.05:
        print(f"HFR деградирует: {hfr_trend:.3f} px/frame")

    # Полная регрессия для предсказания
    slope, intercept = linear_regression(temperature_history)
    predicted_temp = intercept + slope * future_index

    # R² для оценки качества модели
    r2 = calculate_r_squared(values, slope, intercept)

    # Корреляция между метриками
    corr = pearson_correlation(temperature_history, hfr_history)
    if abs(corr) > 0.7:
        print(f"Сильная корреляция: {corr:.2f}")
"""

import logging
from typing import List, Tuple, Optional

logger = logging.getLogger("MathUtils")


def linear_regression(values: List[float]) -> Tuple[float, float]:
    """
    Вычисляет линейную регрессию методом наименьших квадратов.

    X — индексы [0, 1, 2, ..., n-1] (неявная ось времени/кадров).
    Y — переданные значения.

    Формула:
        slope = Σ((x_i - x̄) * (y_i - ȳ)) / Σ((x_i - x̄)²)
        intercept = ȳ - slope * x̄

    Args:
        values: Список числовых значений (минимум 2 элемента
                для осмысленного результата)

    Returns:
        Tuple (slope, intercept):
        - slope: наклон линии (изменение Y на единицу X)
        - intercept: точка пересечения с осью Y

    Edge cases:
        - Пустой список → (0.0, 0.0)
        - 1 элемент → (0.0, values[0])
        - Все значения одинаковы → (0.0, значение)
        - denominator == 0 → (0.0, y_mean)
    """
    n = len(values)
    if n == 0:
        return 0.0, 0.0
    if n == 1:
        return 0.0, values[0]

    # x_mean для индексов [0, 1, ..., n-1] = (n - 1) / 2
    x_mean = (n - 1) / 2.0
    y_mean = sum(values) / n

    numerator = sum((i - x_mean) * (values[i] - y_mean) for i in range(n))
    denominator = sum((i - x_mean) ** 2 for i in range(n))

    if denominator == 0:
        # Все x одинаковы (невозможно при n >= 2) или все y одинаковы
        return 0.0, y_mean

    slope = numerator / denominator
    intercept = y_mean - slope * x_mean
    return slope, intercept


def calculate_trend(values: List[float]) -> float:
    """
    Вычисляет тренд (наклон) последовательности значений.

    Упрощённая версия linear_regression — возвращает только slope.
    Удобно для детекции деградации/улучшения метрик.

    Args:
        values: Список числовых значений

    Returns:
        Slope (наклон):
        - > 0: значения растут
        - < 0: значения убывают
        - = 0: стабильно (или недостаточно данных)

    Edge cases:
        - Менее 2 элементов → 0.0
        - Все значения одинаковы → 0.0
    """
    if len(values) < 2:
        return 0.0
    slope, _ = linear_regression(values)
    return slope


def calculate_r_squared(values: List[float], slope: float, intercept: float) -> float:
    """
    Вычисляет коэффициент детерминации R².

    R² показывает долю дисперсии зависимой переменной,
    объяснённую линейной моделью.

    Формула:
        R² = 1 - SS_res / SS_tot
        где:
        - SS_res = Σ(y_i - ŷ_i)²  (сумма квадратов остатков)
        - SS_tot = Σ(y_i - ȳ)²    (общая сумма квадратов)
        - ŷ_i = slope * i + intercept

    Интерпретация:
        - R² = 1.0: идеальная линейная зависимость
        - R² > 0.7: хорошая линейная модель
        - R² < 0.3: слабая линейная зависимость
        - R² = 0.0: модель не объясняет дисперсию

    Args:
        values: Исходные значения Y
        slope: Наклон линейной модели (из linear_regression)
        intercept: Точка пересечения (из linear_regression)

    Returns:
        R² в диапазоне [0.0, 1.0]. Отрицательные значения
        обрезается до 0.0 (модель хуже, чем просто среднее).
    """
    n = len(values)
    if n < 2:
        return 0.0

    y_mean = sum(values) / n

    # SS_res: сумма квадратов отклонений от предсказанных значений
    ss_res = sum((values[i] - (slope * i + intercept)) ** 2 for i in range(n))

    # SS_tot: общая сумма квадратов отклонений от среднего
    ss_tot = sum((values[i] - y_mean) ** 2 for i in range(n))

    if ss_tot == 0:
        # Все значения одинаковы — модель тривиально точна
        return 1.0 if ss_res == 0 else 0.0

    r_squared = 1.0 - ss_res / ss_tot
    # Обрезаем отрицательные значения (плохая модель)
    return max(0.0, min(1.0, r_squared))


def pearson_correlation(x: List[float], y: List[float]) -> float:
    """
    Вычисляет коэффициент корреляции Пирсона между двумя последовательностями.

    Формула:
        r = Σ((x_i - x̄) * (y_i - ȳ)) / (σ_x * σ_y * n)
        где σ — стандартное отклонение (без деления на n-1, т.к. сокращается)

    Интерпретация:
        - r =  1.0: идеальная положительная корреляция
        - r = -1.0: идеальная отрицательная корреляция
        - r =  0.0: отсутствует линейная корреляция
        - |r| > 0.7: сильная корреляция
        - |r| > 0.5: умеренная корреляция
        - |r| > 0.3: слабая корреляция

    Args:
        x: Первая последовательность
        y: Вторая последовательность

    Returns:
        Коэффициент корреляции в диапазоне [-1.0, 1.0].
        0.0 если недостаточно данных или одна из последовательностей
        имеет нулевую дисперсию.
    """
    n = min(len(x), len(y))
    if n < 3:
        # Для статистической значимости нужно минимум 3 точки
        return 0.0

    # Берём первые n элементов из обеих последовательностей
    x_slice = x[:n]
    y_slice = y[:n]

    x_mean = sum(x_slice) / n
    y_mean = sum(y_slice) / n

    numerator = sum((x_slice[i] - x_mean) * (y_slice[i] - y_mean) for i in range(n))

    x_variance = sum((xi - x_mean) ** 2 for xi in x_slice)
    y_variance = sum((yi - y_mean) ** 2 for yi in y_slice)

    if x_variance == 0 or y_variance == 0:
        # Одна из последовательностей константна — корреляция не определена
        return 0.0

    x_std = x_variance**0.5
    y_std = y_variance**0.5

    return numerator / (x_std * y_std)
