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
    # 1 = объёмное окно взято со сдвигом -1 бар (формирующийся бар отброшен).
    # Объявлять обязательно: датакласс без __slots__ молча принимает любой
    # атрибут, поэтому присваивание неописанного поля в детекторе не падало, а
    # до БД не доезжало — колонка signals.volume_window_shifted стояла NULL у
    # всех сигналов. См. SignalModel.from_detector_signal.
    volume_window_shifted: int | None = None
    # Размах sustain-окна, % — мера шума монеты, по которой считается адаптивный
    # стоп (utils.adaptive_stop_pct). Детектор считает её всё равно, для
    # max_window_range_pct.
    window_range_pct: float | None = None

    # Чем PriceSurgeDetector измерил памп: биржа ряда, рост окна и цены на его
    # краях. PriceSurgeSignalProcessor пишет в БД и в сообщение именно их, а не
    # пересчитывает: пересчёт шёл по свечам без фильтра биржи, и в
    # price_surge_signals попадал другой рост (вплоть до отрицательного), по
    # которому кулдаун затем сравнивал новые пампы — см. docs/strategy.md.
    exchange: str | None = None
    change_pct: float | None = None
    window_open: float | None = None
    window_close: float | None = None


class BaseDetector(ABC):
    """Абстрактный детектор торговых сетапов."""

    @abstractmethod
    async def analyze(self, session) -> list[Signal]:
        """Анализирует данные и возвращает список сигналов."""
        ...
