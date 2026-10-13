"""
Shared utilities for analytics modules.

Extracts duplicated code from detector.py, price_surge.py, app.py, and runner.py.
"""

from datetime import datetime, timedelta

import numpy as np

OI_TREND_BARS = 3  # Legacy-режим: сколько ПОСЛЕДНИХ ТОЧЕК OI берётся на тренд

# Минимум точек во ВРЕМЕННОМ окне (`oi_trend_window_bars > 0`). Три, а не две:
# по двум точкам наклон равен их разнице и целиком определяется шумом последнего
# снимка. Запас на прореживание записи OI есть: даже одна точка на бар даёт в
# 12-минутном окне 4 точки.
OI_TREND_MIN_POINTS = 3


def timeframe_to_minutes(tf: str) -> int:
    """Convert timeframe string (e.g. '3m', '1h') to minutes."""
    if tf.endswith("m"):
        return int(tf[:-1])
    elif tf.endswith("h"):
        return int(tf[:-1]) * 60
    return 3  # sensible default for unknown formats


def calculate_oi_slope_pct(
    values: np.ndarray, x=None, span: float | None = None,
) -> float | None:
    """Наклон OI в % от среднего OI: «сколько процентов прибавляется за окно».

    `x` — положение точек (секунды), `span` — длина ОКНА в тех же единицах.
    Оба заданы — наклон нормируется на окно, и величина перестаёт зависеть от
    того, сколько точек в окно попало. Не заданы — прежнее поведение: `x` —
    номер точки, окно — их количество.

    Зачем нормировать на окно, а не на размах самих точек. Прореживание записи
    OI и колебания каданса меняют и число точек, и их разброс внутри окна;
    нормировка на фиксированное окно оставляет в результате только скорость
    притока, за которой гейт и следит.

    Returns None, если точек меньше двух, окно нулевое или средний OI <= 0.
    """
    if len(values) < 2:
        return None

    if x is None:
        x = np.arange(len(values))
        span = len(values)
    else:
        x = np.asarray(x, dtype=float)
        if span is None:
            span = float(x[-1] - x[0])
        if x[-1] == x[0]:
            return None
    if span is None or span <= 0:
        return None

    slope = np.polyfit(x, values, 1)[0]
    mean_oi = np.mean(values)
    if mean_oi <= 0:
        return None

    return (slope * span) / mean_oi * 100


def oi_trend_passes(
    oi_values, oi_declining_enabled: bool, oi_slope_min_pct: float,
    oi_times=None, window_sec: float | None = None,
) -> tuple[bool, str | None, str | None]:
    """Решение OI-гейта по готовому ряду значений: (прошёл, stage, reason).

    Единственная реализация на боевой детектор и бэктест. До 26.08.2026 движок
    бэктеста повторял эту логику своим кодом, и она дважды разъезжалась с боевой:
    сначала из неё выпала проверка `oi_declining` (завысила прошлые свипы по
    RR/partial-close/retracement), потом — учёт `oi_filter_enabled`. Гейт целиком
    включается флагом `oi_filter_enabled` на стороне вызывающего: здесь только
    содержательная часть, без обращения к БД, чтобы её мог звать и синхронный
    движок, и асинхронный детектор.

    `oi_times` и `window_sec` — временной режим (`oi_trend_window_bars > 0`):
    точки за фиксированное окно вместо фиксированного ИХ ЧИСЛА. Без них режим
    прежний, и он зависит от каданса: строка OI пишется раз в цикл сбора,
    поэтому три точки — это два каданса. Замер 23.09.2026 по медианному размаху
    трёх точек на момент сигнала: 557 с на БД 10-25.08, 187 с на 27.08-22.09,
    139 с на свежей — окно уехало вчетверо от одних перф-фиксов цикла, а вместе
    с ним и смысл порога `oi_slope_min_pct` («+2% за окно» = за 9 мин или за
    2 мин). Замысел был `sustain_bars * timeframe` = 12 минут.
    """
    if oi_values is None:
        return False, None, None

    if oi_times is not None and len(oi_values) < OI_TREND_MIN_POINTS:
        return False, None, None

    # Последняя точка ниже предпоследней — приток уже иссякает
    if oi_declining_enabled and len(oi_values) >= 2 and oi_values[-1] < oi_values[-2]:
        return False, "oi_declining", "OI снижается — последняя точка ниже предпоследней"

    slope_pct = calculate_oi_slope_pct(
        np.asarray(oi_values), x=oi_times, span=window_sec,
    )
    if slope_pct is None:
        return False, None, None

    if slope_pct < oi_slope_min_pct:
        return (
            False,
            "oi_slope_low",
            f"наклон OI {slope_pct:.1f}% < минимума {oi_slope_min_pct}%",
        )

    return True, None, None


# Границы адаптивного стопа, % от цены входа. Не конфиг, потому что это не
# настройка стратегии, а область определения: ниже 2% стоп сидит в
# проскальзывании и комиссии (замер 26 суток: выход по стопу отклоняется от
# заявленного медианно на 0.01 п.п., но круговая комиссия — 0.11% в цене), выше
# 10% сам свип стопов показал ухудшение на обеих БД (3-8%, оптимум 4-5%).
ADAPTIVE_STOP_MIN_PCT = 2.0
ADAPTIVE_STOP_MAX_PCT = 10.0


def adaptive_stop_pct(
    range_pct: float | None,
    mult: float,
    fixed_pct: float,
    min_pct: float = ADAPTIVE_STOP_MIN_PCT,
    max_pct: float = ADAPTIVE_STOP_MAX_PCT,
) -> float:
    """Стоп в % от входа: `mult` размахов sustain-окна, либо фиксированный.

    `mult <= 0` или неизвестный размах — возвращается `fixed_pct`, то есть
    поведение до 23.09.2026. Одна реализация на бэктест и прод: разъехавшаяся
    копия гейта уже дважды стоила неверных выводов (AGENTS.md, правило 2).

    Зачем вообще. Стоп зафиксирован в процентах от цены, а размах sustain-окна у
    монет гуляет от 0.95% до 5.0% — то есть один и тот же стоп стоит от 1.4 до
    3.8 размаха. Замер 23.09.2026 на двух БД: самый шумный квартиль (стоп ~1.4
    размаха, внутри шума) — худший на обеих (E[R] -0.001 против +0.253 на
    27.08-22.09; +0.434 против +0.973 на 10-25.08), корреляция R с отношением
    стоп/шум положительна на обеих (+0.090 и +0.170). Значимости нет ни там, ни
    там — но направление воспроизвелось, а сама величина детектором до сих пор
    не использовалась.

    Это НЕ ATR-адаптивный стоп, отвергнутый в `docs/decisions.md`: там мерой был
    ИСТОРИЧЕСКИЙ ATR, непоказательный в момент пампа. Здесь мера — размах того же
    sustain-окна, который детектор уже считает для `max_window_range_pct`, то
    есть современная сигналу, а не историческая.
    """
    if mult <= 0 or range_pct is None or range_pct <= 0:
        return fixed_pct
    return min(max(range_pct * mult, min_pct), max_pct)


def atr_stop_pct(
    atr_pct: float | None,
    mult: float,
    stop_loss_pct: float,
    min_pct: float = ADAPTIVE_STOP_MIN_PCT,
) -> float:
    """Стоп в % от входа: `mult` × ATR, но НЕ ШИРЕ фиксированного `stop_loss_pct`.

    ATR-стоп 13.10.2026 — пересмотр июньского отказа из `docs/decisions.md`
    («исторический ATR непоказателен в момент пампа») на новых данных: фильтры
    детектора отсекли низколиквидные монеты с аномальной волатильностью.
    Решение о включении — только по свипу на всех БД с правилом 8.

    Кап сверху — сам `stop_loss_pct`, а не отдельный порог: все замеры проекта
    сходятся, что стоп и так широк (4% лучше 5% на обеих БД, свип стопов 23.09,
    год истории 11.10), то есть ATR здесь умеет только СУЖАТЬ стоп под тихую
    монету — вытянуть его на волатильной шире боевого значения он не может.
    Пол `min_pct` тот же, что у адаптивного от размаха: ниже 2% стоп сидит в
    комиссии и проскальзывании (круговая комиссия ≈ 0.11% в цене).

    `mult <= 0` или неизвестный ATR — фиксированный `stop_loss_pct`, поведение
    до 13.10.2026. Одна реализация на бэктест и прод (правило 2).
    """
    if mult <= 0 or atr_pct is None or atr_pct <= 0:
        return stop_loss_pct
    return min(max(atr_pct * mult, min_pct), stop_loss_pct)


def resolve_stop_pct(window_range_pct: float | None, atr_pct: float | None, cfg) -> float:
    """Стоп в % от входа по ОБЕИМ адаптивным мерам сразу: размах окна, затем ATR.

    Механизмы взаимоисключающие (валидатор TradingConfig запрещает включать оба),
    поэтому порядок цепочки не влияет на результат: любой выключенный механизм
    возвращает вход unchanged. Одна реализация на прод (`open_position`) и движок
    (`price_ctx` с теми же ключами) — мера кладётся в контекст детектором.
    """
    stop = adaptive_stop_pct(
        window_range_pct, cfg.stop_loss_window_range_mult, cfg.stop_loss_pct,
    )
    return atr_stop_pct(atr_pct, cfg.stop_loss_atr_mult, stop)


def cb_trade_outcome(pnl: float | None, stop_price: float | None, entry_price: float) -> str:
    """Как закрытая сделка двигает серию Circuit Breaker: 'win' | 'loss' | 'neutral'.

    Плюс сбрасывает серию, минус растит её, а минус при стопе, уже стоящем в
    б/у (`stop_price == entry_price`), нейтрален: это комиссии и проскальзывание
    стопа, а не убыток. Без фиксации доли (partial_close_qty_pct = 0) так выходит
    каждый безубыток, и прежнее `pnl <= 0 → убыток` засчитывало в серию треть
    сделок (свип 08.09.2026). Одна реализация на прод и бэктест — правило 2.
    """
    if (pnl or 0) > 0:
        return "win"
    if stop_price is not None and stop_price == entry_price:
        return "neutral"
    return "loss"


def cb_stop_after_loss(losses: int, stop_until: datetime | None, consumed_at: int,
                       now: datetime, loss_streak_stop: int,
                       stop_minutes: float) -> tuple[datetime | None, int]:
    """Запустить полную остановку Circuit Breaker В МОМЕНТ убытка: `(stop_until, consumed_at)`.

    Зовётся сразу после того, как убыток вырастил серию до `losses`. Остановка
    стартует тогда, когда серия достигла порога, а не когда придёт следующий
    сигнал. До 10.10.2026 прод проверял порог только в `open_position`, и
    60-минутная пауза начиналась с первого сигнала ПОСЛЕ серии — хоть через 15
    часов (02.10 18:39 третий стоп → заблокирован сигнал 03.10 10:12). Движок
    при этом проверял порог на каждом шаге скана, и остановка истекала вхолостую:
    свип CB видел её «мёртвым кодом», а в проде она отсекала первый сетап после
    каждой серии (8 сетапов за 27.08-22.09, 5 за 28.09-10.10).

    Новый убыток сверх уже отработанной остановки (`losses > consumed_at`)
    запускает следующую; если текущая ещё идёт, следующая начинается с её
    конца — так движок вёл себя и раньше. Одна реализация на прод и бэктест.
    """
    if losses < loss_streak_stop or losses <= consumed_at:
        return stop_until, consumed_at
    start = stop_until if stop_until is not None and stop_until > now else now
    return start + timedelta(minutes=stop_minutes), losses


def trailing_scale(cfg, stop_pct: float | None) -> float:
    """Во сколько раз трейлинг-геометрия сжимается вместе с пер-сделочным стопом.

    `trailing_scale_with_stop` выключен или стоп неизвестен — 1.0 (абсолютные
    проценты конфига, как подобрано свипом 11.10.2026). Включён — активация и
    отступ делятся на `stop_loss_pct` и множатся на фактический стоп сделки: при
    стопе 4% пропорции боевые (2× и 1.25× стопа), при стопе 2% — те же 2× и
    1.25×, а не 4× и 2.5×. Без этого на тихих монетах выход уезжает дальше от
    стопа ровно тогда, когда стоп сузился, и б/у-выходов становится больше, а не
    меньше — противоположно цели ATR-стопа.
    """
    if not getattr(cfg, "trailing_scale_with_stop", False) or stop_pct is None:
        return 1.0
    if cfg.stop_loss_pct <= 0:
        return 1.0
    return stop_pct / cfg.stop_loss_pct


def trailing_activation_price(entry_price: float, cfg, stop_pct: float | None = None) -> float:
    """Цена, с которой включается трейлинг-стоп (`trailing_activation_pct` от входа).

    Одна формула на прод (`PositionManager`, `activePrice` ByBit) и движок бэктеста —
    правило 2 AGENTS.md распространяется на любую боевую логику, которую движок
    воспроизводит. `stop_pct` — пер-сделочный стоп ATR: при включённом
    `trailing_scale_with_stop` активация сжимается пропорционально ему.
    """
    return entry_price * (1 + cfg.trailing_activation_pct * trailing_scale(cfg, stop_pct) / 100)


def trailing_distance(entry_price: float, cfg, stop_pct: float | None = None) -> float:
    """Отступ трейлинг-стопа от максимума — ЦЕНОВАЯ дистанция, % от входа.

    Нативный трейлинг ByBit (`trailingStop`) задаётся расстоянием в цене, а не
    процентах от максимума: стоп = максимум после активации − дистанция. Движок
    считает так же, иначе модель описывала бы не тот выход, который исполняет биржа.
    `stop_pct` — см. `trailing_activation_price`.
    """
    return entry_price * cfg.trailing_distance_pct * trailing_scale(cfg, stop_pct) / 100
