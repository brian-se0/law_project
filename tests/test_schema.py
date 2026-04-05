from shadow_trading.schema import canonicalize_columns, missing_core_columns


def test_canonicalize_columns_maps_vendor_headers() -> None:
    raw_columns = [
        "Underlying Symbol",
        "Quote Date",
        "Bid_1545",
        "Ask Size EOD",
        "Implied Volatility 1545",
    ]

    assert canonicalize_columns(raw_columns) == [
        "underlying_symbol",
        "quote_date",
        "bid_1545",
        "ask_size_eod",
        "implied_volatility_1545",
    ]


def test_missing_core_columns_identifies_required_fields() -> None:
    columns = [
        "underlying_symbol",
        "quote_date",
        "root",
        "expiration",
        "strike",
        "option_type",
    ]

    missing = missing_core_columns(columns)

    assert "trade_volume" in missing
    assert "open_interest" in missing
    assert "bid_eod" in missing
