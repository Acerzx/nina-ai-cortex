"""
Image Features Extractor — извлечение признаков из FITS-превью.
Заглушка для идеи 1.1: мультимодальный RAG.

Текущая реализация:
- Извлекает базовые статистики из FITS-заголовков (без загрузки данных)
- ImageFeatureExtractor ABC для будущих CLIP-based моделей

Будущая реализация:
- Генерация preview.jpg из FITS (astropy + PIL)
- CLIP эмбеддинги для изображений
- Новая коллекция Qdrant: nina_images
- Мультимодальный поиск: текст + изображение

Feature flag: feature_flags.rag.multimodal_enabled
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger("ImageFeatures")


@dataclass
class ImageFeatures:
    """Признаки изображения."""

    file_path: str
    wcs_ra: Optional[float] = None
    wcs_dec: Optional[float] = None
    filter_name: Optional[str] = None
    exposure_time: Optional[float] = None
    temperature: Optional[float] = None
    date_obs: Optional[str] = None

    # Будущие поля для CLIP embeddings
    image_embedding: Optional[List[float]] = None
    preview_path: Optional[str] = None

    timestamp: str = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()

    def to_metadata(self) -> Dict[str, Any]:
        """Возвращает метаданные для Qdrant."""
        return {
            "file_path": self.file_path,
            "filter": self.filter_name,
            "exposure_time": self.exposure_time,
            "temperature": self.temperature,
            "date_obs": self.date_obs,
            "has_embedding": self.image_embedding is not None,
        }


class ImageFeatureExtractor(ABC):
    """Абстрактный класс для извлечения признаков изображений."""

    @abstractmethod
    async def extract_from_fits(
        self,
        fits_path: Path,
    ) -> Optional[ImageFeatures]:
        """Извлекает признаки из FITS файла."""
        pass

    @abstractmethod
    async def compute_embedding(
        self,
        features: ImageFeatures,
    ) -> Optional[List[float]]:
        """Вычисляет эмбеддинг изображения."""
        pass


class BasicFITSFeatureExtractor(ImageFeatureExtractor):
    """
    Базовый извлекатель признаков — только FITS-заголовки.
    Не загружает пиксельные данные.
    """

    async def extract_from_fits(
        self,
        fits_path: Path,
    ) -> Optional[ImageFeatures]:
        """Извлекает признаки из FITS заголовков."""
        try:
            from app.core.executors import async_fits_getheader

            header = await async_fits_getheader(fits_path, ext=0)
            if not header:
                return None

            return ImageFeatures(
                file_path=str(fits_path),
                wcs_ra=header.get("CRVAL1"),
                wcs_dec=header.get("CRVAL2"),
                filter_name=header.get("FILTER"),
                exposure_time=header.get("EXPTIME"),
                temperature=header.get("CCD-TEMP") or header.get("TEMPERAT"),
                date_obs=header.get("DATE-OBS"),
            )
        except Exception as e:
            logger.debug(f"Failed to extract FITS features: {e}")
            return None

    async def compute_embedding(
        self,
        features: ImageFeatures,
    ) -> Optional[List[float]]:
        """STUB: возвращает None (будущая CLIP реализация)."""
        return None


class CLIPFeatureExtractorStub(ImageFeatureExtractor):
    """
    STUB для будущего CLIP-based извлекателя.
    Требует: transformers/sentence-transformers, PIL, ~500MB RAM.
    """

    async def extract_from_fits(
        self,
        fits_path: Path,
    ) -> Optional[ImageFeatures]:
        logger.debug("CLIP feature extractor stub called")
        return None

    async def compute_embedding(
        self,
        features: ImageFeatures,
    ) -> Optional[List[float]]:
        return None


# ============================================================================
# FACTORY
# ============================================================================
def create_image_extractor() -> ImageFeatureExtractor:
    """Создаёт image extractor на основе feature flag."""
    multimodal_enabled = False
    try:
        from app.core.config import settings

        ff = getattr(settings, "feature_flags", None)
        if ff:
            rag_ff = getattr(ff, "rag", None)
            if rag_ff:
                multimodal_enabled = getattr(rag_ff, "multimodal_enabled", False)
    except Exception:
        pass

    if multimodal_enabled:
        # В будущем: return CLIPFeatureExtractor()
        pass

    return BasicFITSFeatureExtractor()


# ============================================================================
# SINGLETON INSTANCE
# ============================================================================
image_feature_extractor = create_image_extractor()
