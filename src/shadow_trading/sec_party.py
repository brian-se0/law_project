from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Sequence

import polars as pl

TEXT_PARTY_PATTERNS = [
    (
        re.compile(
            r"\b([A-Z][A-Za-z0-9&.,'()/-]*(?:\s+[A-Z][A-Za-z0-9&.,'()/-]*){0,10})\s+entered into an?\s+Agreement and Plan of Merger\s+with\s+([A-Z][A-Za-z0-9&.,'()/-]*(?:\s+[A-Z][A-Za-z0-9&.,'()/-]*){0,10})",
            re.IGNORECASE,
        ),
        "text_entered_merger_with",
        "target",
        "acquirer",
    ),
    (
        re.compile(
            r"\b([A-Z][A-Za-z0-9&.,'()/-]*(?:\s+[A-Z][A-Za-z0-9&.,'()/-]*){0,10})\s+(?:will|would|is expected to)\s+be acquired by\s+([A-Z][A-Za-z0-9&.,'()/-]*(?:\s+[A-Z][A-Za-z0-9&.,'()/-]*){0,10})",
            re.IGNORECASE,
        ),
        "text_be_acquired_by",
        "target",
        "acquirer",
    ),
    (
        re.compile(
            r"\b([A-Z][A-Za-z0-9&.,'()/-]*(?:\s+[A-Z][A-Za-z0-9&.,'()/-]*){0,10})\s+(?:will|would|expects to|agreed to)\s+acquire\s+([A-Z][A-Za-z0-9&.,'()/-]*(?:\s+[A-Z][A-Za-z0-9&.,'()/-]*){0,10})",
            re.IGNORECASE,
        ),
        "text_acquirer_will_acquire",
        "acquirer",
        "target",
    ),
]

TEXT_TARGET_ONLY_PATTERNS = [
    (
        re.compile(
            r"\boffer to purchase all outstanding shares of\s+([A-Z][A-Za-z0-9&.,'()/-]*(?:\s+[A-Z][A-Za-z0-9&.,'()/-]*){0,12})",
            re.IGNORECASE,
        ),
        "text_offer_purchase_target",
    ),
]

TICKER_SUFFIXES_TO_DROP = {
    "inc",
    "corp",
    "corporation",
    "co",
    "company",
    "ltd",
    "limited",
    "plc",
    "holdings",
    "group",
    "llc",
    "lp",
    "incorporated",
}

PARTY_CAPTURE_SPLIT_PATTERN = re.compile(
    r"\b(?:for|under|pursuant|through|dated|and|at|by and among|upon|to form|for cash)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class CompanyLookupIndex:
    sec_by_cik: dict[str, dict[str, Any]]
    sec_by_ticker: dict[str, dict[str, Any]]
    sec_by_name: dict[str, dict[str, Any]]
    option_by_cik: dict[str, dict[str, Any]]
    option_by_ticker: dict[str, dict[str, Any]]
    option_by_name: dict[str, dict[str, Any]]


@dataclass(frozen=True)
class DealParty:
    cik: str | None
    name: str | None
    ticker: str | None
    underlying_symbol: str | None
    firm_id: str | None
    resolution: str
    has_option_data: bool


@dataclass(frozen=True)
class ResolvedDealParties:
    source: DealParty
    target: DealParty
    acquirer: DealParty | None


def normalize_symbol(value: str) -> str:
    return value.strip().upper().replace("/", ".").replace("-", ".")


def normalize_cik(value: Any) -> str | None:
    if value is None:
        return None
    digits = re.sub(r"\D", "", str(value))
    if not digits:
        return None
    return f"{int(digits):010d}"


def normalize_company_name(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = re.sub(r"[^A-Za-z0-9 ]+", " ", value).lower()
    parts = [part for part in normalized.split() if part not in TICKER_SUFFIXES_TO_DROP]
    return "_".join(parts[:8]) if parts else None


def clean_party_capture(value: str) -> str | None:
    candidate = PARTY_CAPTURE_SPLIT_PATTERN.split(value, maxsplit=1)[0]
    candidate = re.sub(r"\s+", " ", candidate).strip(" ,.;:-")
    if not candidate or len(candidate.split()) > 12:
        return None
    return candidate


def extract_text_party_mentions(plain_text: str) -> dict[str, str | None]:
    for pattern, resolution, first_role, second_role in TEXT_PARTY_PATTERNS:
        match = pattern.search(plain_text)
        if not match:
            continue
        first_party = clean_party_capture(match.group(1))
        second_party = clean_party_capture(match.group(2))
        if not first_party or not second_party:
            continue
        if first_role == "target":
            return {
                "target_name": first_party,
                "target_resolution": resolution,
                "acquirer_name": second_party,
                "acquirer_resolution": resolution,
            }
        return {
            "target_name": second_party,
            "target_resolution": resolution,
            "acquirer_name": first_party,
            "acquirer_resolution": resolution,
        }

    for pattern, resolution in TEXT_TARGET_ONLY_PATTERNS:
        match = pattern.search(plain_text)
        if not match:
            continue
        target_name = clean_party_capture(match.group(1))
        if target_name:
            return {
                "target_name": target_name,
                "target_resolution": resolution,
                "acquirer_name": None,
                "acquirer_resolution": None,
            }

    return {
        "target_name": None,
        "target_resolution": None,
        "acquirer_name": None,
        "acquirer_resolution": None,
    }


def build_company_lookup_index(
    *,
    ticker_frame: pl.DataFrame,
    matched_companies: pl.DataFrame,
) -> CompanyLookupIndex:
    sec_rows = [
        {
            "cik": normalize_cik(row["cik"]),
            "ticker": row["ticker"],
            "name": row["name"],
            "normalized_symbol": row["normalized_symbol"],
            "normalized_company_name": row["normalized_company_name"],
        }
        for row in ticker_frame.select(
            ["cik", "ticker", "name", "normalized_symbol", "normalized_company_name"]
        ).iter_rows(named=True)
    ]
    option_rows = [
        {
            "cik": normalize_cik(row["cik"]),
            "ticker": row["matched_ticker"],
            "name": row["matched_company_name"],
            "normalized_symbol": normalize_symbol(row["matched_ticker"]),
            "normalized_company_name": row["matched_company_slug"],
            "underlying_symbol": row["underlying_symbol"],
            "firm_id": normalize_symbol(row["underlying_symbol"]),
        }
        for row in matched_companies.select(
            [
                "cik",
                "matched_ticker",
                "matched_company_name",
                "matched_company_slug",
                "underlying_symbol",
            ]
        ).iter_rows(named=True)
    ]
    return CompanyLookupIndex(
        sec_by_cik={row["cik"]: row for row in sec_rows if row["cik"]},
        sec_by_ticker=_build_unique_lookup(sec_rows, "normalized_symbol"),
        sec_by_name=_build_unique_lookup(sec_rows, "normalized_company_name"),
        option_by_cik=_build_unique_lookup(option_rows, "cik"),
        option_by_ticker=_build_unique_lookup(option_rows, "normalized_symbol"),
        option_by_name=_build_unique_lookup(option_rows, "normalized_company_name"),
    )


def resolve_deal_parties(
    *,
    matched_company: dict[str, Any],
    form: str,
    header: dict[str, str | None],
    plain_text: str,
    company_lookups: CompanyLookupIndex,
    subject_company_forms: Sequence[str],
) -> ResolvedDealParties:
    matched_party = DealParty(
        cik=normalize_cik(matched_company.get("cik")),
        name=str(matched_company.get("matched_company_name") or ""),
        ticker=str(matched_company.get("matched_ticker") or ""),
        underlying_symbol=str(matched_company.get("underlying_symbol") or ""),
        firm_id=normalize_symbol(str(matched_company.get("underlying_symbol") or "")),
        resolution="matched_option_symbol",
        has_option_data=True,
    )
    text_mentions = extract_text_party_mentions(plain_text)

    target_candidates = [
        (
            _resolve_party_identity(
                cik=header["subject_company_cik"],
                name=header["subject_company_name"],
                resolution="subject_company",
                company_lookups=company_lookups,
            )
            if header["subject_company_name"]
            else None
        ),
        (
            _resolve_party_identity(
                cik=header["filer_cik"],
                name=header["filer_name"],
                resolution="filer",
                company_lookups=company_lookups,
            )
            if header["filer_name"]
            else None
        ),
        (
            _resolve_party_identity(
                name=text_mentions["target_name"],
                resolution=text_mentions["target_resolution"] or "text_target",
                company_lookups=company_lookups,
            )
            if text_mentions["target_name"]
            else None
        ),
        matched_party,
    ]
    target = next(
        (candidate for candidate in target_candidates if candidate is not None), matched_party
    )
    if form in subject_company_forms and target_candidates[0] is not None:
        target = target_candidates[0]

    acquirer_candidates = [
        (
            _resolve_party_identity(
                cik=header["filed_by_cik"],
                name=header["filed_by_name"],
                resolution="filed_by",
                company_lookups=company_lookups,
            )
            if header["filed_by_name"]
            else None
        ),
        (
            _resolve_party_identity(
                cik=header["filer_cik"],
                name=header["filer_name"],
                resolution="filer",
                company_lookups=company_lookups,
            )
            if header["subject_company_name"] and header["filer_name"]
            else None
        ),
        (
            _resolve_party_identity(
                name=text_mentions["acquirer_name"],
                resolution=text_mentions["acquirer_resolution"] or "text_acquirer",
                company_lookups=company_lookups,
            )
            if text_mentions["acquirer_name"]
            else None
        ),
    ]
    acquirer = next(
        (
            candidate
            for candidate in acquirer_candidates
            if candidate is not None and not _same_party(candidate, target)
        ),
        None,
    )
    if target.has_option_data:
        source = target
    elif target.name and matched_party.has_option_data:
        source = DealParty(
            cik=target.cik or matched_party.cik,
            name=target.name,
            ticker=matched_party.ticker,
            underlying_symbol=matched_party.underlying_symbol,
            firm_id=matched_party.firm_id,
            resolution=f"{target.resolution}_matched_option_symbol",
            has_option_data=True,
        )
    else:
        source = matched_party
    return ResolvedDealParties(source=source, target=target, acquirer=acquirer)


def _build_unique_lookup(
    rows: Sequence[dict[str, Any]],
    key_field: str,
) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        key = row.get(key_field)
        if not key:
            continue
        grouped.setdefault(str(key), []).append(row)

    lookup: dict[str, dict[str, Any]] = {}
    for key, group in grouped.items():
        unique_group = {
            (
                row.get("cik"),
                row.get("ticker"),
                row.get("name"),
                row.get("underlying_symbol"),
                row.get("firm_id"),
            ): row
            for row in group
        }
        if len(unique_group) == 1:
            lookup[key] = next(iter(unique_group.values()))
    return lookup


def _resolve_party_identity(
    *,
    cik: str | None = None,
    name: str | None = None,
    ticker: str | None = None,
    resolution: str,
    company_lookups: CompanyLookupIndex,
) -> DealParty:
    normalized_cik = normalize_cik(cik)
    normalized_ticker = normalize_symbol(ticker) if ticker else None
    normalized_name = normalize_company_name(name)

    sec_match = (
        company_lookups.sec_by_cik.get(normalized_cik)
        if normalized_cik
        else (
            company_lookups.sec_by_ticker.get(normalized_ticker)
            if normalized_ticker
            else company_lookups.sec_by_name.get(normalized_name or "")
        )
    )
    option_match = (
        company_lookups.option_by_cik.get(normalized_cik)
        if normalized_cik
        else (
            company_lookups.option_by_ticker.get(normalized_ticker)
            if normalized_ticker
            else company_lookups.option_by_name.get(normalized_name or "")
        )
    )
    return DealParty(
        cik=normalized_cik or (sec_match["cik"] if sec_match else None),
        name=(sec_match["name"] if sec_match else None)
        or name
        or (option_match["name"] if option_match else None),
        ticker=ticker
        or (sec_match["ticker"] if sec_match else None)
        or (option_match["ticker"] if option_match else None),
        underlying_symbol=option_match["underlying_symbol"] if option_match else None,
        firm_id=option_match["firm_id"] if option_match else None,
        resolution=resolution,
        has_option_data=option_match is not None,
    )


def _same_party(left: DealParty | None, right: DealParty | None) -> bool:
    if left is None or right is None:
        return False
    if left.cik and right.cik:
        return left.cik == right.cik
    left_name = normalize_company_name(left.name)
    right_name = normalize_company_name(right.name)
    if left_name and right_name:
        return left_name == right_name
    if left.ticker and right.ticker:
        return normalize_symbol(left.ticker) == normalize_symbol(right.ticker)
    return False
