from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class Signal:
    symbol: str
    setup_type: str
    direction: str  # "long" | "short"
    confidence: int  # 1–100
    message: str


class BaseDetector(ABC):
    """Абстрактный детектор торговых сетапов."""

    @abstractmethod
    async def analyze(self, session) -> list[Signal]:
        """Анализирует данные и возвращает список сигналов."""
        ...
