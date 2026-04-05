from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

MARKET_OPEN = time(9, 30)
MARKET_CLOSE = time(16, 0)


@dataclass(frozen=True)
class EventAlignment:
    announcement_dt: datetime
    local_announcement_dt: datetime
    event_trading_date: date
    pre_event_window_end: date
    during_market_hours: bool
    market_open: time
    market_close: time


def align_announcement_timestamp(
    announcement_dt: datetime,
    timezone: str = "America/New_York",
    market_open: time = MARKET_OPEN,
    market_close: time = MARKET_CLOSE,
) -> EventAlignment:
    try:
        tz = ZoneInfo(timezone)
        local_dt = (
            announcement_dt.replace(tzinfo=tz)
            if announcement_dt.tzinfo is None
            else announcement_dt.astimezone(tz)
        )
    except ZoneInfoNotFoundError:
        local_dt = _fallback_localize(announcement_dt, timezone)
    announcement_date = local_dt.date()
    clock_time = local_dt.time().replace(tzinfo=None)
    trading_day = is_trading_day(announcement_date)
    during_market_hours = trading_day and market_open <= clock_time < market_close

    if trading_day and clock_time < market_open:
        event_trading_date = announcement_date
    elif during_market_hours:
        event_trading_date = announcement_date
    elif trading_day:
        event_trading_date = next_trading_day(announcement_date + timedelta(days=1))
    else:
        event_trading_date = next_trading_day(announcement_date)

    return EventAlignment(
        announcement_dt=announcement_dt,
        local_announcement_dt=local_dt,
        event_trading_date=event_trading_date,
        pre_event_window_end=previous_trading_day(event_trading_date),
        during_market_hours=during_market_hours,
        market_open=market_open,
        market_close=market_close,
    )


def is_trading_day(day: date) -> bool:
    return day.weekday() < 5 and day not in us_market_holidays(day.year)


def next_trading_day(day: date) -> date:
    candidate = day
    while not is_trading_day(candidate):
        candidate += timedelta(days=1)
    return candidate


def previous_trading_day(day: date) -> date:
    candidate = day - timedelta(days=1)
    while not is_trading_day(candidate):
        candidate -= timedelta(days=1)
    return candidate


def us_market_holidays(year: int) -> set[date]:
    holidays = {
        _observed(date(year, 1, 1)),
        _nth_weekday_of_month(year, 1, 0, 3),
        _nth_weekday_of_month(year, 2, 0, 3),
        _good_friday(year),
        _last_weekday_of_month(year, 5, 0),
        _observed(date(year, 7, 4)),
        _nth_weekday_of_month(year, 9, 0, 1),
        _nth_weekday_of_month(year, 11, 3, 4),
        _observed(date(year, 12, 25)),
    }
    if year >= 2022:
        holidays.add(_observed(date(year, 6, 19)))
    return holidays


def _observed(day: date) -> date:
    if day.weekday() == 5:
        return day - timedelta(days=1)
    if day.weekday() == 6:
        return day + timedelta(days=1)
    return day


def _nth_weekday_of_month(year: int, month: int, weekday: int, nth: int) -> date:
    first_day = date(year, month, 1)
    days_until = (weekday - first_day.weekday()) % 7
    return first_day + timedelta(days=days_until + (nth - 1) * 7)


def _last_weekday_of_month(year: int, month: int, weekday: int) -> date:
    if month == 12:
        candidate = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        candidate = date(year, month + 1, 1) - timedelta(days=1)
    while candidate.weekday() != weekday:
        candidate -= timedelta(days=1)
    return candidate


def _good_friday(year: int) -> date:
    return _easter_sunday(year) - timedelta(days=2)


def _easter_sunday(year: int) -> date:
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    offset_l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * offset_l) // 451
    month = (h + offset_l - 7 * m + 114) // 31
    day = ((h + offset_l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def _fallback_localize(announcement_dt: datetime, timezone_name: str) -> datetime:
    if timezone_name != "America/New_York":
        return announcement_dt

    if announcement_dt.tzinfo is None:
        offset = _eastern_utc_offset_hours(announcement_dt.date())
        return announcement_dt.replace(tzinfo=timezone(timedelta(hours=offset)))

    utc_dt = announcement_dt.astimezone(UTC)
    local_date = (utc_dt + timedelta(hours=-5)).date()
    offset = _eastern_utc_offset_hours(local_date)
    eastern_tz = timezone(timedelta(hours=offset))
    return utc_dt.astimezone(eastern_tz)


def _eastern_utc_offset_hours(local_day: date) -> int:
    return -4 if _is_us_eastern_dst(local_day) else -5


def _is_us_eastern_dst(local_day: date) -> bool:
    year = local_day.year
    if year >= 2007:
        start = _nth_weekday_of_month(year, 3, 6, 2)
        end = _nth_weekday_of_month(year, 11, 6, 1)
    else:
        start = _nth_weekday_of_month(year, 4, 6, 1)
        end = _last_weekday_of_month(year, 10, 6)
    return start <= local_day < end
