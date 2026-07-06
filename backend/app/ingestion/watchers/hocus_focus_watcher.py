import logging
from pathlib import Path
from app.ingestion.watchers.base import BaseFileWatcher, event_bus
from app.ingestion.parsers.hocus_focus import parse_hocus_focus_csv, filter_anomalies
from app.core.config import settings

logger = logging.getLogger("HocusFocusWatcher")


class HocusFocusWatcher(BaseFileWatcher):
    """
    Мониторит CSV-отчеты Hocus Focus.
    Устраняет Упрощение #2: анализирует КАЖДУЮ звезду и применяет Z-Score фильтрацию.
    """

    def __init__(self):
        # Путь берется из конфига (в будущем будет динамически извлекаться из XML-профиля N.I.N.A.)
        # Для примера используем хардкод из settings.yaml, который нужно добавить
        hf_path = Path(r"C:\Users\istep\YandexDisk\Хобби\Астрономия\ПО\N.I.N.A\Data\HF")
        super().__init__(
            watch_path=hf_path,
            target_files=["*.csv"],  # Отслеживаем все CSV
        )
        # Расширяем target_files для базового класса, чтобы он пропускал только CSV
        self.handler.target_files = [".csv"]

    async def process_file(self, path: Path) -> None:
        if path.suffix.lower() != ".csv":
            return

        logger.info(f"Parsing Hocus Focus report: {path.name}")
        stars = parse_hocus_focus_csv(path)

        if not stars:
            logger.warning(f"No stars found in {path.name}")
            return

        report = filter_anomalies(stars)
        report.file_name = path.stem

        logger.info(
            f"HF Analysis [{path.stem}]: Total={report.total_stars_detected}, "
            f"Valid={report.valid_stars_count}, Anomalies={report.anomalies_count}, "
            f"Median FWHM={report.median_fwhm:.2f}"
            if report.median_fwhm
            else "N/A"
        )

        payload = {
            "file_name": report.file_name,
            "report": report.model_dump(
                exclude={"stars"}
            ),  # В EventBus отправляем только агрегаты, чтобы не забивать память
        }
        await event_bus.publish("HOCUS_FOCUS_ANALYSIS", payload)
