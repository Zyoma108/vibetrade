import logging
from datetime import datetime, timezone

from sqlalchemy import select

from src.analytics.base import BaseDetector, Signal
from src.config import StrategyConfig

logger = logging.getLogger(__name__)


class SetupDetector(BaseDetector):
    """Заглушка детектора. Реальная стратегия будет реализована позже."""

    def __init__(self, config: StrategyConfig):
        self.config = config

    async def analyze(self, session) -> list[Signal]:
        """
        Анализирует данные в БД и возвращает сигналы.

        На данном этапе — заглушка. Всегда возвращает пустой список.
        Когда стратегия будет готова, здесь будет:
        - Загрузка последних свечей
        - Расчёт индикаторов
        - Проверка условий сетапа
        - Формирование сигналов
        """
        logger.debug("Анализ сетапов... (заглушка)")
        return []
