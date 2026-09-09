from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class Signal:
    symbol: str
    setup_type: str
    direction: str  # "long" | "short"
    confidence: int  # 1–100
    message: str

    # Замер «формирующегося бара»: прошёл бы тот же сетап на окне без ещё не
    # закрытого последнего бара. Заполняет только SetupDetector, на решение о
    # входе не влияет — см. docs/strategy.md, «Формирующийся бар».
    closed_bar_ok: bool | None = None      # None = вердикт не определён
    closed_bar_stage: str | None = None    # гейт, на котором отказ
    last_bar_age_sec: int | None = None    # возраст последнего бара окна, с


class BaseDetector(ABC):
    """Абстрактный детектор торговых сетапов."""

    @abstractmethod
    async def analyze(self, session) -> list[Signal]:
        """Анализирует данные и возвращает список сигналов."""
        ...
