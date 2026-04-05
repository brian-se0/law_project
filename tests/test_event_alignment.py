from datetime import datetime

from shadow_trading.calendars import align_announcement_timestamp, next_trading_day


def test_align_announcement_timestamp_during_market_hours() -> None:
    alignment = align_announcement_timestamp(datetime.fromisoformat("2024-03-15T10:00:00-04:00"))

    assert alignment.event_trading_date.isoformat() == "2024-03-15"
    assert alignment.pre_event_window_end.isoformat() == "2024-03-14"
    assert alignment.during_market_hours is True


def test_align_announcement_timestamp_after_close_rolls_forward() -> None:
    alignment = align_announcement_timestamp(datetime.fromisoformat("2024-03-15T17:01:00-04:00"))

    assert alignment.event_trading_date.isoformat() == "2024-03-18"
    assert alignment.pre_event_window_end.isoformat() == "2024-03-15"
    assert alignment.during_market_hours is False


def test_good_friday_is_not_treated_as_trading_day() -> None:
    assert (
        next_trading_day(datetime.fromisoformat("2024-03-29T12:00:00").date()).isoformat()
        == "2024-04-01"
    )


def test_align_announcement_timestamp_utc_after_close_maps_to_next_day() -> None:
    alignment = align_announcement_timestamp(datetime.fromisoformat("2024-04-01T20:05:00+00:00"))

    assert alignment.event_trading_date.isoformat() == "2024-04-02"
    assert alignment.pre_event_window_end.isoformat() == "2024-04-01"
