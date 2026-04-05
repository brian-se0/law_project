from __future__ import annotations

import re

CORE_REQUIRED_COLUMNS = {
    "underlying_symbol",
    "quote_date",
    "root",
    "expiration",
    "strike",
    "option_type",
    "trade_volume",
    "bid_1545",
    "ask_1545",
    "underlying_bid_1545",
    "underlying_ask_1545",
    "bid_eod",
    "ask_eod",
    "underlying_bid_eod",
    "underlying_ask_eod",
    "vwap",
    "open_interest",
}

VENDOR_BASE_COLUMNS = {
    "underlying_symbol",
    "quote_date",
    "root",
    "expiration",
    "strike",
    "option_type",
    "open",
    "high",
    "low",
    "close",
    "trade_volume",
    "bid_size_1545",
    "bid_1545",
    "ask_size_1545",
    "ask_1545",
    "underlying_bid_1545",
    "underlying_ask_1545",
    "implied_underlying_price_1545",
    "bid_size_eod",
    "bid_eod",
    "ask_size_eod",
    "ask_eod",
    "underlying_bid_eod",
    "underlying_ask_eod",
    "vwap",
    "open_interest",
    "delivery_code",
}

VENDOR_CALCS_COLUMNS = {
    "active_underlying_price_1545",
    "implied_volatility_1545",
    "delta_1545",
    "gamma_1545",
    "theta_1545",
    "vega_1545",
    "rho_1545",
}

VENDOR_EXPECTED_COLUMNS = VENDOR_BASE_COLUMNS | VENDOR_CALCS_COLUMNS

STRING_COLUMNS = {
    "underlying_symbol",
    "root",
    "option_type",
    "delivery_code",
}

DATE_COLUMNS = {
    "quote_date",
    "expiration",
}

INTEGER_COLUMNS = {
    "trade_volume",
    "bid_size_1545",
    "ask_size_1545",
    "bid_size_eod",
    "ask_size_eod",
    "open_interest",
}

FLOAT_COLUMNS = {
    "strike",
    "open",
    "high",
    "low",
    "close",
    "bid_1545",
    "ask_1545",
    "underlying_bid_1545",
    "underlying_ask_1545",
    "implied_underlying_price_1545",
    "active_underlying_price_1545",
    "implied_volatility_1545",
    "delta_1545",
    "gamma_1545",
    "theta_1545",
    "vega_1545",
    "rho_1545",
    "bid_eod",
    "ask_eod",
    "underlying_bid_eod",
    "underlying_ask_eod",
    "vwap",
}

CONTRACT_DAY_KEY = [
    "quote_date",
    "underlying_symbol",
    "root",
    "expiration",
    "strike",
    "option_type",
]


_ALIASES = {
    "underlyingsymbol": "underlying_symbol",
    "quote_date": "quote_date",
    "quotedate": "quote_date",
    "root": "root",
    "expiration": "expiration",
    "expiry": "expiration",
    "strike": "strike",
    "strikeprice": "strike",
    "optiontype": "option_type",
    "option_type": "option_type",
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "trade_volume": "trade_volume",
    "tradevolume": "trade_volume",
    "volume": "trade_volume",
    "bid_size_1545": "bid_size_1545",
    "bidsize1545": "bid_size_1545",
    "bid_1545": "bid_1545",
    "bid1545": "bid_1545",
    "ask_size_1545": "ask_size_1545",
    "asksize1545": "ask_size_1545",
    "ask_1545": "ask_1545",
    "ask1545": "ask_1545",
    "underlying_bid_1545": "underlying_bid_1545",
    "underlyingbid1545": "underlying_bid_1545",
    "underlying_ask_1545": "underlying_ask_1545",
    "underlyingask1545": "underlying_ask_1545",
    "implied_underlying_price_1545": "implied_underlying_price_1545",
    "impliedunderlyingprice1545": "implied_underlying_price_1545",
    "active_underlying_price_1545": "active_underlying_price_1545",
    "activeunderlyingprice1545": "active_underlying_price_1545",
    "implied_volatility_1545": "implied_volatility_1545",
    "impliedvolatility1545": "implied_volatility_1545",
    "delta_1545": "delta_1545",
    "delta1545": "delta_1545",
    "gamma_1545": "gamma_1545",
    "gamma1545": "gamma_1545",
    "theta_1545": "theta_1545",
    "theta1545": "theta_1545",
    "vega_1545": "vega_1545",
    "vega1545": "vega_1545",
    "rho_1545": "rho_1545",
    "rho1545": "rho_1545",
    "bid_size_eod": "bid_size_eod",
    "bidsizeeod": "bid_size_eod",
    "bid_eod": "bid_eod",
    "bideod": "bid_eod",
    "ask_size_eod": "ask_size_eod",
    "asksizeeod": "ask_size_eod",
    "ask_eod": "ask_eod",
    "askeod": "ask_eod",
    "underlying_bid_eod": "underlying_bid_eod",
    "underlyingbideod": "underlying_bid_eod",
    "underlying_ask_eod": "underlying_ask_eod",
    "underlyingaskeod": "underlying_ask_eod",
    "vwap": "vwap",
    "open_interest": "open_interest",
    "openinterest": "open_interest",
    "delivery_code": "delivery_code",
    "deliverycode": "delivery_code",
}


def canonicalize_column_name(column_name: str) -> str:
    compact = re.sub(r"[^a-z0-9]+", "", column_name.strip().lower())
    return _ALIASES.get(compact, re.sub(r"[^a-z0-9]+", "_", column_name.strip().lower()).strip("_"))


def canonicalize_columns(columns: list[str]) -> list[str]:
    return [canonicalize_column_name(column) for column in columns]


def missing_core_columns(columns: list[str]) -> list[str]:
    canonical_columns = set(canonicalize_columns(columns))
    return sorted(CORE_REQUIRED_COLUMNS - canonical_columns)
