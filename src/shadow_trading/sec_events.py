from __future__ import annotations

import html
import json
import re
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Sequence
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import polars as pl

from shadow_trading.calendars import align_announcement_timestamp
from shadow_trading.sec_party import (
    build_company_lookup_index,
    clean_party_capture,
    extract_text_party_mentions,
    normalize_cik,
    normalize_company_name,
    normalize_symbol,
    resolve_deal_parties,
)

COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
SUBMISSIONS_BASE_URL = "https://data.sec.gov/submissions"
SEC_ARCHIVES_BASE_URL = "https://www.sec.gov/Archives/edgar/data"
BROWSE_EDGAR_COMPANY_SEARCH_URL = "https://www.sec.gov/cgi-bin/browse-edgar"
FULL_TEXT_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"
FULL_TEXT_SEARCH_PAGE_SIZE = 100
COMPANY_SEARCH_RESULT_LIMIT = 40

PROXY_FORMS = {"DEFA14A", "DEFM14A", "PREM14A", "DEFA14C", "DEFM14C", "PREM14C"}
TENDER_FORMS = {"SC TO-T", "SC TO-T/A", "SC TO-C", "14D9", "14D9/A"}
REGISTRATION_FORMS = {"425", "S-4", "S-4/A", "F-4", "F-4/A"}
SUBJECT_COMPANY_FORMS = TENDER_FORMS
TARGET_SIDE_FORMS = PROXY_FORMS | {"14D9", "14D9/A"}

STRONG_MNA_PATTERNS = [
    (
        re.compile(r"\bagreement and plan of merger\b", re.IGNORECASE),
        "agreement_and_plan_of_merger",
    ),
    (re.compile(r"\bmerger agreement\b", re.IGNORECASE), "merger_agreement"),
    (re.compile(r"\bmerger consideration\b", re.IGNORECASE), "merger_consideration"),
    (re.compile(r"\btender offer\b", re.IGNORECASE), "tender_offer"),
    (re.compile(r"\boffer(?:s|ing)? to purchase\b", re.IGNORECASE), "offer_to_purchase"),
    (re.compile(r"\bto be acquired by\b", re.IGNORECASE), "to_be_acquired_by"),
    (re.compile(r"\bwill be acquired by\b", re.IGNORECASE), "will_be_acquired_by"),
    (re.compile(r"\bacquired by\b", re.IGNORECASE), "acquired_by"),
    (re.compile(r"\bwholly owned subsidiary of\b", re.IGNORECASE), "wholly_owned_subsidiary"),
    (re.compile(r"\bspecial meeting of stockholders\b", re.IGNORECASE), "special_meeting"),
]

SUPPORTING_PATTERNS = [
    (re.compile(r"\btransaction agreement\b", re.IGNORECASE), "transaction_agreement"),
    (re.compile(r"\bproposed merger\b", re.IGNORECASE), "proposed_merger"),
    (re.compile(r"\bmerger proposal\b", re.IGNORECASE), "merger_proposal"),
]

EXCLUSION_PATTERNS = [
    (re.compile(r"\basset purchase agreement\b", re.IGNORECASE), "asset_purchase_agreement"),
    (re.compile(r"\bacquired certain assets\b", re.IGNORECASE), "acquired_certain_assets"),
    (re.compile(r"\basset acquisition\b", re.IGNORECASE), "asset_acquisition"),
]

COUNTERPARTY_PATTERNS = [
    re.compile(
        r"\b(?:to be acquired by|will be acquired by|acquired by)\s+([A-Z][A-Za-z0-9&.,'()/-]*(?:\s+[A-Z][A-Za-z0-9&.,'()/-]*){0,10})",
        re.IGNORECASE,
    ),
    re.compile(
        r"\bagreement and plan of merger(?:[^.]{0,200}?)with\s+([A-Z][A-Za-z0-9&.,'()/-]*(?:\s+[A-Z][A-Za-z0-9&.,'()/-]*){0,10})",
        re.IGNORECASE,
    ),
    re.compile(
        r"\bwholly owned subsidiary of\s+([A-Z][A-Za-z0-9&.,'()/-]*(?:\s+[A-Z][A-Za-z0-9&.,'()/-]*){0,10})",
        re.IGNORECASE,
    ),
]

HEADER_LABELS = {
    "filer": "FILER",
    "filed_by": "FILED BY",
    "subject_company": "SUBJECT COMPANY",
}

SYMBOL_EXCHANGE_PATTERN = (
    r"(?:NASDAQ|Nasdaq(?:GS|GM|CM)?|NYSE(?:\s+American)?|AMEX|OTCQX|OTCQB|OTC(?:\s+Pink)?)"
)
HISTORICAL_SYMBOL_SEARCH_EXCHANGES = ("NASDAQ", "NYSE", "NYSE American", "AMEX")


@dataclass
class SecClient:
    user_agent: str
    request_spacing_seconds: float
    cache_root: Path
    refresh_cache: bool = False
    _last_request_ts: float = 0.0

    def fetch_json(self, url: str, cache_path: Path) -> Any:
        if cache_path.exists() and not self.refresh_cache:
            return json.loads(cache_path.read_text(encoding="utf-8"))
        text = self.fetch_text(url, cache_path=None)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(text, encoding="utf-8")
        return json.loads(text)

    def fetch_text(self, url: str, cache_path: Path | None) -> str:
        if cache_path is not None and cache_path.exists() and not self.refresh_cache:
            return cache_path.read_text(encoding="utf-8", errors="replace")
        self._throttle()
        request = Request(url, headers={"User-Agent": self.user_agent})
        with urlopen(request, timeout=60) as response:
            text = response.read().decode("utf-8", errors="replace")
        if cache_path is not None:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(text, encoding="utf-8")
        return text

    def _throttle(self) -> None:
        now = time.monotonic()
        wait = self.request_spacing_seconds - (now - self._last_request_ts)
        if wait > 0:
            time.sleep(wait)
        self._last_request_ts = time.monotonic()


def build_sec_event_candidates(
    *,
    underlyings_path: Path,
    cache_root: Path,
    user_agent: str,
    request_spacing_seconds: float,
    start_date: date,
    end_date: date,
    candidate_forms: Sequence[str],
    limit_companies: int | None = None,
    symbol_filter: Sequence[str] | None = None,
    refresh_cache: bool = False,
) -> tuple[pl.DataFrame, dict[str, Any]]:
    client = SecClient(
        user_agent=user_agent,
        request_spacing_seconds=request_spacing_seconds,
        cache_root=cache_root,
        refresh_cache=refresh_cache,
    )

    option_symbols = load_option_symbol_universe(
        underlyings_path=underlyings_path,
        start_date=start_date,
        end_date=end_date,
        symbol_filter=symbol_filter,
    )
    ticker_frame = fetch_company_ticker_frame(client)
    matched_companies = match_option_symbols_to_sec_companies(option_symbols, ticker_frame)
    historical_resolutions = resolve_historical_sec_companies(
        client=client,
        option_symbols=option_symbols,
        matched_companies=matched_companies,
        start_date=start_date,
        end_date=end_date,
        candidate_forms=candidate_forms,
    )
    historical_resolution_count = historical_resolutions.height
    if historical_resolutions.height:
        matched_companies = pl.concat(
            [matched_companies, historical_resolutions.select(matched_companies.columns)],
            how="vertical_relaxed",
        )
        matched_companies = matched_companies.unique(subset=["underlying_symbol", "cik"])
    company_lookups = build_company_lookup_index(
        ticker_frame=ticker_frame,
        matched_companies=matched_companies,
    )
    if limit_companies is not None:
        matched_companies = matched_companies.head(limit_companies)

    candidate_rows: list[dict[str, Any]] = []
    companies_scanned = 0
    for company in matched_companies.iter_rows(named=True):
        companies_scanned += 1
        filing_rows = fetch_candidate_filings_for_company(
            client=client,
            company=company,
            start_date=start_date,
            end_date=end_date,
            candidate_forms=set(candidate_forms),
        )
        for filing_row in filing_rows:
            candidate_rows.append(
                enrich_candidate_filing(
                    client=client,
                    company=company,
                    filing_row=filing_row,
                    company_lookups=company_lookups,
                )
            )

    candidates = _candidate_frame(candidate_rows).filter(pl.col("source_has_option_data"))
    metadata = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "option_symbol_count": option_symbols.height,
        "matched_company_count": matched_companies.height,
        "historical_resolution_count": historical_resolution_count,
        "companies_scanned": companies_scanned,
    }
    return candidates, metadata


def build_sec_event_universe(candidates: pl.DataFrame) -> pl.DataFrame:
    if candidates.height == 0:
        return _empty_event_frame()

    filtered = candidates.filter(pl.col("is_mna_candidate") & pl.col("is_target_side"))
    if filtered.height == 0:
        return _empty_event_frame()

    rows = sorted(
        filtered.iter_rows(named=True),
        key=lambda row: (
            str(row["source_cik"]),
            _parse_datetime_or_min(row["acceptance_datetime_utc"]),
            str(row["accession_number"]),
        ),
    )
    clusters: list[list[dict[str, Any]]] = []
    for row in rows:
        matched_cluster = _find_compatible_cluster(clusters, row)
        if matched_cluster is None:
            clusters.append([row])
        else:
            matched_cluster.append(row)

    event_rows = [
        _cluster_to_event_row(cluster, event_index)
        for event_index, cluster in enumerate(clusters, start=1)
    ]
    return pl.from_dicts(event_rows).sort(["source_cik", "first_public_disclosure_dt"])


def build_sec_event_qc_report(
    *,
    candidates: pl.DataFrame,
    events: pl.DataFrame,
    candidates_output_path: Path,
    events_output_path: Path,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    form_counts = (
        candidates.group_by("form").len().sort("len", descending=True).iter_rows(named=True)
        if candidates.height
        else []
    )
    return {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "start_date": metadata["start_date"],
        "end_date": metadata["end_date"],
        "option_symbol_count": metadata["option_symbol_count"],
        "matched_company_count": metadata["matched_company_count"],
        "historical_resolution_count": metadata.get("historical_resolution_count", 0),
        "companies_scanned": metadata["companies_scanned"],
        "candidate_filing_count": candidates.height,
        "mna_candidate_count": (
            candidates.filter(pl.col("is_mna_candidate")).height if candidates.height else 0
        ),
        "target_side_candidate_count": (
            candidates.filter(pl.col("is_target_side")).height if candidates.height else 0
        ),
        "target_match_count": (
            candidates.filter(pl.col("target_name").is_not_null()).height
            if candidates.height
            else 0
        ),
        "acquirer_match_count": (
            candidates.filter(pl.col("acquirer_name").is_not_null()).height
            if candidates.height
            else 0
        ),
        "source_firm_id_count": (
            candidates.filter(pl.col("source_firm_id").is_not_null()).height
            if candidates.height
            else 0
        ),
        "event_count": events.height,
        "manual_review_event_count": (
            events.filter(pl.col("requires_manual_review")).height if events.height else 0
        ),
        "candidates_output": str(candidates_output_path),
        "events_output": str(events_output_path),
        "form_counts": {row["form"]: int(row["len"]) for row in form_counts},
        "provenance_note": (
            "Built from official SEC company ticker mappings, issuer submissions JSON, and raw filing text. "
            "The pipeline restricts the universe to option-underlying symbols observed in the processed data, "
            "classifies a transparent set of M&A-related forms with keyword rules, preserves filing timestamps, "
            "extracts target/acquirer fields when possible, and flags ambiguous clusters for manual review rather "
            "than silently forcing a deal interpretation."
        ),
    }


def render_sec_event_qc_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# SEC M&A Event Universe QC",
        "",
        f"- Generated at (UTC): {report['generated_at_utc']}",
        f"- Date range: {report['start_date']} to {report['end_date']}",
        f"- Option symbols considered: {report['option_symbol_count']:,}",
        f"- Matched SEC companies: {report['matched_company_count']:,}",
        f"- Historical SEC resolutions: {report['historical_resolution_count']:,}",
        f"- Companies scanned: {report['companies_scanned']:,}",
        f"- Candidate filings: {report['candidate_filing_count']:,}",
        f"- Classified M&A candidates: {report['mna_candidate_count']:,}",
        f"- Target-side candidates: {report['target_side_candidate_count']:,}",
        f"- Candidates with extracted targets: {report['target_match_count']:,}",
        f"- Candidates with extracted acquirers: {report['acquirer_match_count']:,}",
        f"- Candidates with canonical source firm IDs: {report['source_firm_id_count']:,}",
        f"- Final events: {report['event_count']:,}",
        f"- Manual-review events: {report['manual_review_event_count']:,}",
        f"- Candidate table: `{report['candidates_output']}`",
        f"- Event table: `{report['events_output']}`",
        "",
        "## Forms",
        "",
    ]
    for form, count in report["form_counts"].items():
        lines.append(f"- {form}: {count:,}")
    lines.extend(["", "## Provenance", "", report["provenance_note"], ""])
    return "\n".join(lines)


def load_option_symbol_universe(
    underlyings_path: Path,
    start_date: date,
    end_date: date,
    symbol_filter: Sequence[str] | None = None,
) -> pl.DataFrame:
    if not underlyings_path.exists():
        raise FileNotFoundError(
            f"{underlyings_path} does not exist. Build the underlying-daily table before building events."
        )
    frame = pl.read_parquet(underlyings_path, columns=["quote_date", "underlying_symbol"])
    filtered = frame.filter(
        (pl.col("quote_date") >= pl.lit(start_date)) & (pl.col("quote_date") <= pl.lit(end_date))
    )
    if symbol_filter:
        normalized_symbols = [normalize_symbol(symbol) for symbol in symbol_filter]
        filtered = filtered.filter(
            _normalized_symbol_expr("underlying_symbol").is_in(normalized_symbols)
        )
    return (
        filtered.select("underlying_symbol")
        .unique()
        .sort("underlying_symbol")
        .with_columns(_normalized_symbol_expr("underlying_symbol").alias("normalized_symbol"))
    )


def fetch_company_ticker_frame(client: SecClient) -> pl.DataFrame:
    cache_path = client.cache_root / "company_tickers_exchange.json"
    payload = client.fetch_json(COMPANY_TICKERS_URL, cache_path)
    return pl.DataFrame(payload["data"], schema=payload["fields"], orient="row").with_columns(
        [
            pl.col("cik").cast(pl.Int64),
            pl.col("ticker").cast(pl.String),
            pl.col("name").cast(pl.String),
            pl.col("exchange").cast(pl.String),
            _normalized_symbol_expr("ticker").alias("normalized_symbol"),
            pl.col("name")
            .map_elements(normalize_company_name, return_dtype=pl.String)
            .alias("normalized_company_name"),
        ]
    )


def match_option_symbols_to_sec_companies(
    option_symbols: pl.DataFrame,
    ticker_frame: pl.DataFrame,
) -> pl.DataFrame:
    return (
        option_symbols.join(
            ticker_frame.select(
                [
                    "cik",
                    "name",
                    "ticker",
                    "exchange",
                    "normalized_symbol",
                    "normalized_company_name",
                ]
            ),
            on="normalized_symbol",
            how="inner",
        )
        .rename(
            {
                "name": "matched_company_name",
                "ticker": "matched_ticker",
                "exchange": "matched_exchange",
            }
        )
        .sort(["underlying_symbol", "cik"])
        .with_columns(
            [
                pl.col("matched_company_name")
                .map_elements(normalize_company_name, return_dtype=pl.String)
                .alias("matched_company_slug")
            ]
        )
    )


def resolve_historical_sec_companies(
    *,
    client: SecClient,
    option_symbols: pl.DataFrame,
    matched_companies: pl.DataFrame,
    start_date: date,
    end_date: date,
    candidate_forms: Sequence[str],
    max_results_per_symbol: int = 40,
) -> pl.DataFrame:
    unmatched_symbols = option_symbols.join(
        matched_companies.select("normalized_symbol").unique(),
        on="normalized_symbol",
        how="anti",
    )
    if unmatched_symbols.height == 0:
        return _empty_historical_resolution_frame()

    rows: list[dict[str, Any]] = []
    for symbol_row in unmatched_symbols.iter_rows(named=True):
        resolved = _resolve_historical_company_for_symbol(
            client=client,
            underlying_symbol=str(symbol_row["underlying_symbol"]),
            normalized_symbol=str(symbol_row["normalized_symbol"]),
            start_date=start_date,
            end_date=end_date,
            candidate_forms=candidate_forms,
            max_results=max_results_per_symbol,
        )
        if resolved is not None:
            rows.append(resolved)

    if not rows:
        return _empty_historical_resolution_frame()
    return (
        pl.from_dicts(rows)
        .with_columns(
            pl.col("matched_company_name")
            .map_elements(normalize_company_name, return_dtype=pl.String)
            .alias("matched_company_slug")
        )
        .sort(["underlying_symbol", "cik"])
    )


def _resolve_historical_company_for_symbol(
    *,
    client: SecClient,
    underlying_symbol: str,
    normalized_symbol: str,
    start_date: date,
    end_date: date,
    candidate_forms: Sequence[str],
    max_results: int,
) -> dict[str, Any] | None:
    candidate_rows: list[dict[str, Any]] = []
    for search_query in build_historical_symbol_search_queries(normalized_symbol):
        search_hits = _search_full_text_symbol_hits(
            client=client,
            symbol=normalized_symbol,
            query=search_query["query"],
            cache_label=search_query["cache_label"],
            start_date=start_date,
            end_date=end_date,
            candidate_forms=candidate_forms,
            max_results=max_results,
        )
        for hit in search_hits:
            candidate_rows.extend(
                _historical_company_candidates_from_search_hit(
                    client=client,
                    symbol=normalized_symbol,
                    hit=hit,
                    search_cache_label=search_query["cache_label"],
                )
            )
            best = select_historical_company_candidate(candidate_rows)
            if best is not None:
                break
        best = select_historical_company_candidate(candidate_rows)
        if best is not None:
            break
    else:
        best = select_historical_company_candidate(candidate_rows)
        if best is None:
            return None

    return {
        "underlying_symbol": underlying_symbol,
        "normalized_symbol": normalized_symbol,
        "cik": int(best["cik"]),
        "matched_company_name": best["matched_company_name"],
        "matched_ticker": normalized_symbol,
        "matched_exchange": None,
        "normalized_company_name": normalize_company_name(best["matched_company_name"]),
        "historical_resolution_source": best["match_source"],
        "historical_resolution_score": int(best["support_score"]),
        "historical_resolution_hit_count": int(best["support_hit_count"]),
        "historical_resolution_accessions": ";".join(sorted(set(best["supporting_accessions"]))),
    }


def build_historical_symbol_search_queries(symbol: str) -> list[dict[str, str]]:
    targeted_queries = [{"cache_label": "trading_symbol", "query": f'"symbol {symbol}"'}]
    for exchange in HISTORICAL_SYMBOL_SEARCH_EXCHANGES:
        exchange_label = re.sub(r"[^A-Za-z0-9]+", "_", exchange).strip("_").lower()
        targeted_queries.append(
            {
                "cache_label": f"exchange_{exchange_label}",
                "query": f'"{exchange}: {symbol}"',
            }
        )
    symbol_query = {"cache_label": "symbol", "query": symbol}
    queries = (
        [*targeted_queries, symbol_query] if len(symbol) <= 4 else [symbol_query, *targeted_queries]
    )

    deduped: list[dict[str, str]] = []
    seen_queries: set[str] = set()
    for query in queries:
        if query["query"] in seen_queries:
            continue
        deduped.append(query)
        seen_queries.add(query["query"])
    return deduped


def select_historical_company_candidate(
    candidate_rows: Sequence[dict[str, Any]],
) -> dict[str, Any] | None:
    if not candidate_rows:
        return None

    aggregated: dict[str, dict[str, Any]] = {}
    for row in candidate_rows:
        cik = normalize_cik(row.get("cik"))
        if cik is None:
            continue
        existing = aggregated.get(cik)
        if existing is None:
            aggregated[cik] = {
                "cik": cik,
                "matched_company_name": row["matched_company_name"],
                "match_source": row["match_source"],
                "support_score": int(row["support_score"]),
                "support_hit_count": 1,
                "strong_match_count": 1 if int(row["support_score"]) >= 90 else 0,
                "supporting_accessions": [row["supporting_accession_number"]],
            }
            continue

        existing["support_hit_count"] += 1
        existing["supporting_accessions"].append(row["supporting_accession_number"])
        if int(row["support_score"]) >= 90:
            existing["strong_match_count"] += 1
        if int(row["support_score"]) > int(existing["support_score"]):
            existing["support_score"] = int(row["support_score"])
            existing["match_source"] = row["match_source"]
            existing["matched_company_name"] = row["matched_company_name"]

    ranked = sorted(
        aggregated.values(),
        key=lambda row: (
            int(row["support_score"]),
            int(row["strong_match_count"]),
            int(row["support_hit_count"]),
            row["matched_company_name"],
        ),
        reverse=True,
    )
    best = ranked[0]
    if len(ranked) == 1:
        return best

    runner_up = ranked[1]
    if historical_company_candidates_are_ambiguous(best, runner_up):
        return None
    return best


def historical_company_candidates_are_ambiguous(
    best: dict[str, Any],
    runner_up: dict[str, Any],
) -> bool:
    if best["cik"] == runner_up["cik"]:
        return False

    best_score = int(best["support_score"])
    runner_up_score = int(runner_up["support_score"])
    best_strong = int(best["strong_match_count"])
    runner_up_strong = int(runner_up["strong_match_count"])
    best_hits = int(best["support_hit_count"])
    runner_up_hits = int(runner_up["support_hit_count"])

    if best_score >= runner_up_score + 10:
        return False
    if best_strong >= runner_up_strong + 2:
        return False
    if best_hits >= max(2, 2 * runner_up_hits):
        return False
    return (
        runner_up_score >= best_score - 5
        and runner_up_strong >= max(0, best_strong - 1)
        and runner_up_hits >= max(1, int(best_hits * 0.75))
    )


def _search_full_text_symbol_hits(
    *,
    client: SecClient,
    symbol: str,
    query: str,
    cache_label: str,
    start_date: date,
    end_date: date,
    candidate_forms: Sequence[str],
    max_results: int,
) -> list[dict[str, Any]]:
    deduped_hits: dict[str, dict[str, Any]] = {}
    form_list = ",".join(normalize_full_text_search_forms(candidate_forms))
    for offset in range(0, max_results, FULL_TEXT_SEARCH_PAGE_SIZE):
        params = {
            "q": query,
            "forms": form_list,
            "startdt": start_date.isoformat(),
            "enddt": end_date.isoformat(),
            "from": str(offset),
        }
        cache_path = (
            client.cache_root
            / "full_text_search"
            / (
                f"{symbol}_{cache_label}_{start_date.isoformat()}_{end_date.isoformat()}_{offset}.json"
            )
        )
        payload = client.fetch_json(f"{FULL_TEXT_SEARCH_URL}?{urlencode(params)}", cache_path)
        page_hits = payload.get("hits", {}).get("hits", [])
        for hit in page_hits:
            hit_id = str(hit.get("_id", ""))
            if hit_id:
                deduped_hits[hit_id] = hit

        total = int(payload.get("hits", {}).get("total", {}).get("value", 0))
        if len(
            page_hits
        ) < FULL_TEXT_SEARCH_PAGE_SIZE or offset + FULL_TEXT_SEARCH_PAGE_SIZE >= min(
            total, max_results
        ):
            break
    return list(deduped_hits.values())


def normalize_full_text_search_forms(candidate_forms: Sequence[str]) -> list[str]:
    normalized_forms = {
        re.sub(r"/A$", "", form.strip().upper())
        for form in candidate_forms
        if form and form.strip()
    }
    return sorted(normalized_forms)


def _historical_company_candidates_from_search_hit(
    *,
    client: SecClient,
    symbol: str,
    hit: dict[str, Any],
    search_cache_label: str = "symbol",
) -> list[dict[str, Any]]:
    source = hit.get("_source", {})
    accession_number = str(source.get("adsh") or "").strip()
    if not accession_number:
        hit_id = str(hit.get("_id", ""))
        accession_number = hit_id.split(":", maxsplit=1)[0] if ":" in hit_id else hit_id

    ciks = source.get("ciks") or []
    filing_cik = normalize_cik(ciks[0] if ciks else None)
    if not accession_number or filing_cik is None:
        return []

    raw_filing_url = build_raw_filing_url(filing_cik, accession_number)
    cache_path = client.cache_root / "filings" / filing_cik / f"{accession_number}.txt"
    raw_text = client.fetch_text(raw_filing_url, cache_path)
    header = extract_filing_header(raw_text)
    plain_text = filing_text_to_plain_text(raw_text)
    symbol_context_names = extract_symbol_context_company_names(plain_text, symbol)
    trading_symbol_match = filing_mentions_trading_symbol(raw_text, symbol)
    targeted_query = search_cache_label != "symbol"
    display_name_candidates = parse_display_name_candidates(
        source.get("display_names") or [],
        symbol,
        allow_single_name_match=targeted_query,
        allow_symbol_name_match=targeted_query,
    )
    company_search_candidates = resolve_symbol_context_company_candidates(
        client=client,
        context_names=symbol_context_names,
    )

    candidates: list[dict[str, Any]] = []
    if header["subject_company_cik"] and header["subject_company_name"]:
        if company_name_matches_context(header["subject_company_name"], symbol_context_names):
            candidates.append(
                {
                    "cik": header["subject_company_cik"],
                    "matched_company_name": header["subject_company_name"],
                    "match_source": "historical_subject_company",
                    "support_score": 100,
                    "supporting_accession_number": accession_number,
                }
            )
    if header["filer_cik"] and header["filer_name"]:
        if company_name_matches_context(header["filer_name"], symbol_context_names):
            candidates.append(
                {
                    "cik": header["filer_cik"],
                    "matched_company_name": header["filer_name"],
                    "match_source": "historical_filer_context",
                    "support_score": 90,
                    "supporting_accession_number": accession_number,
                }
            )
        elif trading_symbol_match:
            candidates.append(
                {
                    "cik": header["filer_cik"],
                    "matched_company_name": header["filer_name"],
                    "match_source": "historical_filer_trading_symbol",
                    "support_score": 80,
                    "supporting_accession_number": accession_number,
                }
            )
    if (
        header["filed_by_cik"]
        and header["filed_by_name"]
        and company_name_matches_context(header["filed_by_name"], symbol_context_names)
    ):
        candidates.append(
            {
                "cik": header["filed_by_cik"],
                "matched_company_name": header["filed_by_name"],
                "match_source": "historical_filed_by_context",
                "support_score": 85,
                "supporting_accession_number": accession_number,
            }
        )

    for display_candidate in display_name_candidates:
        candidates.append(
            {
                "cik": display_candidate["cik"],
                "matched_company_name": display_candidate["name"],
                "match_source": display_candidate["match_source"],
                "support_score": display_candidate["support_score"],
                "supporting_accession_number": accession_number,
            }
        )
    for company_search_candidate in company_search_candidates:
        candidates.append(
            {
                "cik": company_search_candidate["cik"],
                "matched_company_name": company_search_candidate["matched_company_name"],
                "match_source": company_search_candidate["match_source"],
                "support_score": company_search_candidate["support_score"],
                "supporting_accession_number": accession_number,
            }
        )
    return candidates


def resolve_symbol_context_company_candidates(
    *,
    client: SecClient,
    context_names: Sequence[str],
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for context_name in context_names:
        for candidate in search_sec_company_candidates_by_name(
            client=client, company_name=context_name
        ):
            key = (candidate["cik"], candidate["matched_company_name"])
            if key in seen:
                continue
            seen.add(key)
            candidates.append(candidate)
    return candidates


def search_sec_company_candidates_by_name(
    *,
    client: SecClient,
    company_name: str,
) -> list[dict[str, Any]]:
    search_name = clean_party_capture(company_name)
    if not search_name:
        return []

    cache_slug = normalize_company_name(search_name) or re.sub(r"[^A-Za-z0-9]+", "_", search_name)
    params = {
        "action": "getcompany",
        "company": search_name,
        "owner": "exclude",
        "count": str(COMPANY_SEARCH_RESULT_LIMIT),
    }
    cache_path = client.cache_root / "company_search" / f"{cache_slug}.html"
    html_text = client.fetch_text(
        f"{BROWSE_EDGAR_COMPANY_SEARCH_URL}?{urlencode(params)}", cache_path
    )

    direct_candidates = parse_sec_company_search_direct_candidates(
        html_text=html_text,
        company_name=search_name,
    )
    if direct_candidates:
        return direct_candidates
    return parse_sec_company_search_result_candidates(html_text=html_text, company_name=search_name)


def parse_sec_company_search_direct_candidates(
    *,
    html_text: str,
    company_name: str,
) -> list[dict[str, Any]]:
    match = re.search(
        r'<span class="companyName">\s*(.*?)\s*<acronym[^>]*>CIK</acronym>#:\s*<a[^>]*>(\d+)',
        html_text,
        re.IGNORECASE | re.DOTALL,
    )
    if match is None:
        return []

    current_name = normalize_company_search_name(match.group(1))
    cik = normalize_cik(match.group(2))
    if current_name is None or cik is None:
        return []

    candidates: list[dict[str, Any]] = []
    if company_name_matches_search_query(current_name, company_name):
        candidates.append(
            {
                "cik": cik,
                "matched_company_name": current_name,
                "match_source": "historical_company_search_current_name",
                "support_score": 95,
            }
        )

    former_names = extract_sec_company_former_names(html_text)
    for former_name in former_names:
        if company_name_matches_search_query(former_name, company_name):
            candidates.append(
                {
                    "cik": cik,
                    "matched_company_name": former_name,
                    "match_source": "historical_company_search_former_name",
                    "support_score": 94,
                }
            )
    return candidates


def parse_sec_company_search_result_candidates(
    *,
    html_text: str,
    company_name: str,
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for cik_value, raw_name in re.findall(
        r"<td[^>]*>\s*<a[^>]*>(\d+)</a>\s*</td>\s*<td[^>]*>\s*(.*?)\s*(?:<br\s*/?>|</td>)",
        html_text,
        re.IGNORECASE | re.DOTALL,
    ):
        parsed_name = normalize_company_search_name(raw_name)
        cik = normalize_cik(cik_value)
        if parsed_name is None or cik is None:
            continue
        if not company_name_matches_search_query(parsed_name, company_name):
            continue
        candidates.append(
            {
                "cik": cik,
                "matched_company_name": parsed_name,
                "match_source": "historical_company_search_results",
                "support_score": 92,
            }
        )
    return candidates


def extract_sec_company_former_names(html_text: str) -> list[str]:
    matches = re.findall(
        r"formerly:\s*(.*?)\s*\(filings through",
        html_text,
        re.IGNORECASE | re.DOTALL,
    )
    former_names = [normalize_company_search_name(match) for match in matches]
    return [former_name for former_name in former_names if former_name]


def normalize_company_search_name(value: str) -> str | None:
    unescaped = html.unescape(value)
    without_tags = re.sub(r"<[^>]+>", " ", unescaped)
    normalized = re.sub(r"\s+", " ", without_tags).strip(" ,.;:-")
    return normalized or None


def company_name_matches_search_query(company_name: str, query_name: str) -> bool:
    normalized_company = normalize_company_name(company_name)
    normalized_query = normalize_company_name(query_name)
    if normalized_company is None or normalized_query is None:
        return False
    if normalized_company == normalized_query:
        return True

    company_parts = normalized_company.split("_")
    query_parts = normalized_query.split("_")
    if len(query_parts) >= 2 and company_parts[: len(query_parts)] == query_parts:
        return True
    return False


def extract_symbol_context_company_names(plain_text: str, symbol: str) -> list[str]:
    patterns = [
        re.compile(
            rf"([A-Z][A-Za-z0-9&.,'()/-]*(?:\s+[A-Z][A-Za-z0-9&.,'()/-]*){{0,10}})\s*\(\s*{SYMBOL_EXCHANGE_PATTERN}\s*:\s*{re.escape(symbol)}\s*\)",
            re.IGNORECASE,
        ),
        re.compile(
            rf"([A-Z][A-Za-z0-9&.,'()/-]*(?:\s+[A-Z][A-Za-z0-9&.,'()/-]*){{0,10}})\s*\(\s*{re.escape(symbol)}\s*\)",
            re.IGNORECASE,
        ),
        re.compile(
            rf"([A-Z][A-Za-z0-9&.,'()/-]*(?:\s+[A-Z][A-Za-z0-9&.,'()/-]*){{0,10}})\s+(?:common stock|ordinary shares|common shares|shares|stock)\s+(?:is|are)\s+traded\b[^.]+?\bunder the symbol\s+[\"“”']?{re.escape(symbol)}[\"“”']?",
            re.IGNORECASE,
        ),
        re.compile(
            rf"([A-Z][A-Za-z0-9&.,'()/-]*(?:\s+[A-Z][A-Za-z0-9&.,'()/-]*){{0,10}})\s+(?:common stock|ordinary shares|common shares|shares|stock)\s+trades\b[^.]+?\bunder the symbol\s+[\"“”']?{re.escape(symbol)}[\"“”']?",
            re.IGNORECASE,
        ),
    ]
    matches: list[str] = []
    for pattern in patterns:
        for match in pattern.finditer(plain_text):
            candidate = clean_party_capture(match.group(1))
            if candidate:
                matches.append(trim_symbol_context_candidate(candidate))
    deduped = sorted({match for match in matches if match})
    return deduped


def filing_mentions_trading_symbol(raw_text: str, symbol: str) -> bool:
    normalized_symbol = re.escape(symbol)
    patterns = [
        re.compile(
            rf"Trading Symbol(?:\(s\))?[^A-Za-z0-9]{{0,200}}{normalized_symbol}\b",
            re.IGNORECASE | re.DOTALL,
        ),
        re.compile(
            rf"<(?:dei:TradingSymbol|ix:nonFraction)[^>]*>\s*{normalized_symbol}\s*</",
            re.IGNORECASE,
        ),
    ]
    return any(pattern.search(raw_text) for pattern in patterns)


def parse_display_name_candidates(
    display_names: Sequence[str],
    symbol: str,
    *,
    allow_single_name_match: bool = False,
    allow_symbol_name_match: bool = False,
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    parsed_display_names: list[tuple[str, str]] = []
    for raw_value in display_names:
        cik_match = re.search(r"\(CIK\s+(\d+)\)", raw_value, re.IGNORECASE)
        if not cik_match:
            continue
        cik = normalize_cik(cik_match.group(1))
        if cik is None:
            continue
        pre_cik = raw_value[: cik_match.start()].strip()
        ticker_match = re.search(r"\(([^()]*)\)\s*$", pre_cik)
        tickers: set[str] = set()
        name = pre_cik
        if ticker_match:
            tickers = {
                normalize_symbol(token.strip())
                for token in ticker_match.group(1).split(",")
                if token.strip()
            }
            name = pre_cik[: ticker_match.start()].strip()

        parsed_display_names.append((cik, name))
        if normalize_symbol(symbol) in tickers:
            candidates.append(
                {
                    "cik": cik,
                    "name": name,
                    "match_source": "historical_display_name_ticker",
                    "support_score": 95,
                }
            )
            continue
        if allow_symbol_name_match and display_name_mentions_symbol(name, symbol):
            candidates.append(
                {
                    "cik": cik,
                    "name": name,
                    "match_source": "historical_display_name_symbol_hint",
                    "support_score": 94,
                }
            )

    if allow_single_name_match and not candidates and len(parsed_display_names) == 1:
        cik, name = parsed_display_names[0]
        candidates.append(
            {
                "cik": cik,
                "name": name,
                "match_source": "historical_display_name_single_match",
                "support_score": 93,
            }
        )
    return candidates


def display_name_mentions_symbol(company_name: str, symbol: str) -> bool:
    normalized_company = normalize_company_name(company_name)
    normalized_symbol = normalize_symbol(symbol).lower()
    if normalized_company is None or not normalized_symbol:
        return False
    company_tokens = normalized_company.split("_")
    return any(
        token == normalized_symbol or token.startswith(normalized_symbol)
        for token in company_tokens
    )


def company_name_matches_context(company_name: str, context_names: Sequence[str]) -> bool:
    normalized_company = normalize_company_name(company_name)
    if normalized_company is None:
        return False
    for context_name in context_names:
        normalized_context = normalize_company_name(context_name)
        if normalized_context is None:
            continue
        if normalized_company == normalized_context:
            return True
        if normalized_company in normalized_context or normalized_context in normalized_company:
            return True
    return False


def trim_symbol_context_candidate(value: str) -> str:
    candidate = re.sub(
        r"^.*?\b(?:will acquire|to acquire|acquire|acquired)\b\s+",
        "",
        value,
        flags=re.IGNORECASE,
    )
    candidate = re.sub(
        r"^.*?\b(?:announced(?:\s+that)?|announce|delivered|submitted|make|made)\b\s+",
        "",
        candidate,
        flags=re.IGNORECASE,
    )
    candidate = re.sub(
        r"\b(?:common stock|ordinary shares|common shares|shares|stock|common)\b\s*$",
        "",
        candidate,
        flags=re.IGNORECASE,
    )
    candidate = re.sub(r"\s+", " ", candidate).strip(" ,.;:-")
    return candidate


def fetch_candidate_filings_for_company(
    *,
    client: SecClient,
    company: dict[str, Any],
    start_date: date,
    end_date: date,
    candidate_forms: set[str],
) -> list[dict[str, Any]]:
    cik = int(company["cik"])
    main_payload = client.fetch_json(
        f"{SUBMISSIONS_BASE_URL}/CIK{cik:010d}.json",
        client.cache_root / "submissions" / f"CIK{cik:010d}.json",
    )
    rows = filing_rows_from_payload(main_payload["filings"]["recent"], cik, company)

    for history_file in main_payload["filings"].get("files", []):
        history_start = date.fromisoformat(history_file["filingFrom"])
        history_end = date.fromisoformat(history_file["filingTo"])
        if history_end < start_date or history_start > end_date:
            continue
        history_payload = client.fetch_json(
            f"{SUBMISSIONS_BASE_URL}/{history_file['name']}",
            client.cache_root / "submissions" / history_file["name"],
        )
        rows.extend(filing_rows_from_payload(history_payload, cik, company))

    unique_rows: dict[str, dict[str, Any]] = {}
    for row in rows:
        filing_date = date.fromisoformat(row["filing_date"])
        if filing_date < start_date or filing_date > end_date:
            continue
        if row["form"] not in candidate_forms:
            continue
        unique_rows[row["accession_number"]] = row
    return sorted(
        unique_rows.values(), key=lambda row: (row["filing_date"], row["accession_number"])
    )


def filing_rows_from_payload(
    payload: dict[str, list[Any]],
    cik: int,
    company: dict[str, Any],
) -> list[dict[str, Any]]:
    accession_numbers = payload.get("accessionNumber", [])
    rows: list[dict[str, Any]] = []
    for index, accession_number in enumerate(accession_numbers):
        rows.append(
            {
                "matched_symbol": company["underlying_symbol"],
                "matched_cik": cik,
                "matched_company_name": company["matched_company_name"],
                "matched_ticker": company["matched_ticker"],
                "matched_exchange": company["matched_exchange"],
                "accession_number": accession_number,
                "filing_date": payload["filingDate"][index],
                "acceptance_datetime_utc": payload["acceptanceDateTime"][index],
                "form": payload["form"][index],
                "items": payload["items"][index] or "",
                "primary_document": payload["primaryDocument"][index] or "",
                "primary_doc_description": payload["primaryDocDescription"][index] or "",
            }
        )
    return rows


def enrich_candidate_filing(
    *,
    client: SecClient,
    company: dict[str, Any],
    filing_row: dict[str, Any],
    company_lookups,
) -> dict[str, Any]:
    raw_filing_url = build_raw_filing_url(filing_row["matched_cik"], filing_row["accession_number"])
    cache_path = (
        client.cache_root
        / "filings"
        / f"{int(filing_row['matched_cik']):010d}"
        / f"{filing_row['accession_number']}.txt"
    )
    raw_text = client.fetch_text(raw_filing_url, cache_path)
    header = extract_filing_header(raw_text)
    plain_text = filing_text_to_plain_text(raw_text)
    classification = classify_filing_text(
        form=filing_row["form"],
        items=filing_row["items"],
        primary_doc_description=filing_row["primary_doc_description"],
        plain_text=plain_text,
    )
    parties = resolve_deal_parties(
        matched_company=company,
        form=str(filing_row["form"]),
        header=header,
        plain_text=plain_text,
        company_lookups=company_lookups,
        subject_company_forms=tuple(SUBJECT_COMPANY_FORMS),
    )
    acceptance_datetime = header["acceptance_datetime"] or filing_row["acceptance_datetime_utc"]
    acquirer_name = parties.acquirer.name if parties.acquirer else None
    is_target_side = classification["is_target_side"] or (
        classification["is_mna_candidate"]
        and parties.target.name is not None
        and parties.acquirer is not None
    )
    return {
        **filing_row,
        "raw_filing_url": raw_filing_url,
        "filer_name": header["filer_name"],
        "filer_cik": header["filer_cik"],
        "filed_by_name": header["filed_by_name"],
        "filed_by_cik": header["filed_by_cik"],
        "subject_company_name": header["subject_company_name"],
        "subject_company_cik": header["subject_company_cik"],
        "source_firm_id": parties.source.firm_id,
        "source_cik": parties.source.cik,
        "source_name": parties.source.name,
        "source_ticker": parties.source.ticker,
        "source_underlying_symbol": parties.source.underlying_symbol,
        "source_resolution": parties.source.resolution,
        "source_has_option_data": parties.source.has_option_data,
        "target_firm_id": parties.target.firm_id,
        "target_cik": parties.target.cik,
        "target_name": parties.target.name,
        "target_ticker": parties.target.ticker,
        "target_underlying_symbol": parties.target.underlying_symbol,
        "target_resolution": parties.target.resolution,
        "target_has_option_data": parties.target.has_option_data,
        "acquirer_firm_id": parties.acquirer.firm_id if parties.acquirer else None,
        "acquirer_cik": parties.acquirer.cik if parties.acquirer else None,
        "acquirer_name": acquirer_name,
        "acquirer_ticker": parties.acquirer.ticker if parties.acquirer else None,
        "acquirer_underlying_symbol": (
            parties.acquirer.underlying_symbol if parties.acquirer else None
        ),
        "acquirer_resolution": parties.acquirer.resolution if parties.acquirer else None,
        "acquirer_has_option_data": parties.acquirer.has_option_data if parties.acquirer else False,
        "acceptance_datetime_utc": acceptance_datetime,
        "counterparty_name": acquirer_name,
        "counterparty_slug": normalize_counterparty_name(acquirer_name) if acquirer_name else None,
        "deal_type": classification["deal_type"],
        "mna_match_score": classification["score"],
        "matched_keyword_count": len(classification["matched_keywords"]),
        "matched_keywords": ";".join(classification["matched_keywords"]),
        "exclusion_hits": ";".join(classification["exclusion_hits"]),
        "text_excerpt": classification["excerpt"],
        "is_mna_candidate": classification["is_mna_candidate"],
        "is_target_side": is_target_side,
        "requires_manual_review": (
            classification["requires_manual_review"]
            or parties.acquirer is None
            or not parties.target.has_option_data
        ),
    }


def build_raw_filing_url(cik: int | str, accession_number: str) -> str:
    return f"{SEC_ARCHIVES_BASE_URL}/{int(cik)}/{accession_number}.txt"


def extract_filing_header(raw_text: str) -> dict[str, str | None]:
    return {
        "acceptance_datetime": _extract_acceptance_datetime(raw_text),
        "filer_name": _extract_header_company_field(raw_text, "filer", "COMPANY CONFORMED NAME"),
        "filer_cik": normalize_cik(
            _extract_header_company_field(raw_text, "filer", "CENTRAL INDEX KEY")
        ),
        "filed_by_name": _extract_header_company_field(
            raw_text, "filed_by", "COMPANY CONFORMED NAME"
        ),
        "filed_by_cik": normalize_cik(
            _extract_header_company_field(raw_text, "filed_by", "CENTRAL INDEX KEY")
        ),
        "subject_company_name": _extract_header_company_field(
            raw_text, "subject_company", "COMPANY CONFORMED NAME"
        ),
        "subject_company_cik": normalize_cik(
            _extract_header_company_field(raw_text, "subject_company", "CENTRAL INDEX KEY")
        ),
    }


def filing_text_to_plain_text(raw_text: str) -> str:
    document_text = raw_text.split("</SEC-HEADER>", maxsplit=1)[-1]
    unescaped = html.unescape(document_text)
    no_tags = re.sub(r"<[^>]+>", " ", unescaped)
    normalized = re.sub(r"\s+", " ", no_tags)
    return normalized.strip()


def classify_filing_text(
    *,
    form: str,
    items: str,
    primary_doc_description: str,
    plain_text: str,
) -> dict[str, Any]:
    matched_keywords = [
        label for pattern, label in STRONG_MNA_PATTERNS if pattern.search(plain_text)
    ]
    supporting_keywords = [
        label for pattern, label in SUPPORTING_PATTERNS if pattern.search(plain_text)
    ]
    exclusion_hits = [label for pattern, label in EXCLUSION_PATTERNS if pattern.search(plain_text)]

    score = 0
    if form in PROXY_FORMS:
        score += 1
    if form in TENDER_FORMS:
        score += 2
    if form in REGISTRATION_FORMS:
        score += 1
    if form.startswith("8-K") and any(item in items for item in ("1.01", "2.01", "8.01")):
        score += 1
    score += 2 * len(matched_keywords) + len(supporting_keywords)
    score -= 3 * len(exclusion_hits)

    is_mna_candidate = score >= 3 and (bool(matched_keywords) or form in TENDER_FORMS)
    is_target_side = (
        form in TARGET_SIDE_FORMS
        or "to_be_acquired_by" in matched_keywords
        or "will_be_acquired_by" in matched_keywords
        or "acquired_by" in matched_keywords
        or form in TENDER_FORMS
    )
    requires_manual_review = is_mna_candidate and not is_target_side

    if (
        form in TENDER_FORMS
        or "tender_offer" in matched_keywords
        or "offer_to_purchase" in matched_keywords
    ):
        deal_type = "tender_offer"
    elif matched_keywords or form in PROXY_FORMS or form in REGISTRATION_FORMS:
        deal_type = "merger"
    else:
        deal_type = "other"

    excerpt = extract_keyword_excerpt(plain_text, matched_keywords or supporting_keywords)
    return {
        "matched_keywords": matched_keywords + supporting_keywords,
        "exclusion_hits": exclusion_hits,
        "score": score,
        "is_mna_candidate": is_mna_candidate,
        "is_target_side": is_target_side,
        "requires_manual_review": requires_manual_review,
        "deal_type": deal_type,
        "excerpt": excerpt or primary_doc_description,
    }


def resolve_source_company(
    *,
    matched_cik: int,
    matched_name: str,
    matched_ticker: str,
    form: str,
    header: dict[str, str | None],
) -> tuple[str, str, str]:
    if (
        form in SUBJECT_COMPANY_FORMS
        and header["subject_company_cik"]
        and header["subject_company_name"]
    ):
        return (
            str(header["subject_company_cik"]),
            str(header["subject_company_name"]),
            "subject_company",
        )
    if header["filer_cik"] and header["filer_name"]:
        return str(header["filer_cik"]), str(header["filer_name"]), "filer"
    return (
        normalize_cik(matched_cik) or str(matched_cik),
        matched_name or matched_ticker,
        "matched_ticker",
    )


def resolve_counterparty_name(
    *,
    header: dict[str, str | None],
    source_name: str,
    source_resolution: str,
    plain_text: str,
) -> str | None:
    if source_resolution == "subject_company" and header["filed_by_name"]:
        return str(header["filed_by_name"])

    extracted = extract_text_party_mentions(plain_text)
    if extracted["acquirer_name"] and normalize_counterparty_name(
        extracted["acquirer_name"]
    ) != normalize_counterparty_name(source_name):
        return extracted["acquirer_name"]

    for pattern in COUNTERPARTY_PATTERNS:
        match = pattern.search(plain_text)
        if match:
            candidate = clean_party_capture(match.group(1))
            if candidate and normalize_counterparty_name(candidate) != normalize_counterparty_name(
                source_name
            ):
                return candidate
    return None


def clean_counterparty_capture(value: str) -> str | None:
    return clean_party_capture(value)


def extract_keyword_excerpt(plain_text: str, keyword_labels: Sequence[str]) -> str | None:
    if not keyword_labels:
        return None
    keyword_map = {label: pattern for pattern, label in STRONG_MNA_PATTERNS + SUPPORTING_PATTERNS}
    for label in keyword_labels:
        pattern = keyword_map.get(label)
        if pattern is None:
            continue
        match = pattern.search(plain_text)
        if match:
            start = max(0, match.start() - 120)
            end = min(len(plain_text), match.end() + 240)
            return plain_text[start:end]
    return None


def normalize_counterparty_name(value: str | None) -> str | None:
    return normalize_company_name(value)


def _normalized_symbol_expr(column: str) -> pl.Expr:
    return (
        pl.col(column)
        .cast(pl.String)
        .str.strip_chars()
        .str.to_uppercase()
        .str.replace_all("/", ".", literal=True)
        .str.replace_all("-", ".", literal=True)
    )


def _extract_acceptance_datetime(raw_text: str) -> str | None:
    match = re.search(r"<ACCEPTANCE-DATETIME>(\d{14})", raw_text)
    if not match:
        return None
    dt = datetime.strptime(match.group(1), "%Y%m%d%H%M%S").replace(tzinfo=UTC)
    return dt.isoformat()


def _extract_header_company_field(raw_text: str, label_key: str, field_name: str) -> str | None:
    lines = raw_text.splitlines()
    try:
        header_end_index = next(
            index for index, line in enumerate(lines) if line.strip().startswith("</SEC-HEADER>")
        )
    except StopIteration:
        header_end_index = len(lines)

    label = f"{HEADER_LABELS[label_key]}:"
    label_indices = [
        index for index, line in enumerate(lines[:header_end_index]) if line.strip() == label
    ]
    if not label_indices:
        return None
    start_index = label_indices[0] + 1
    for line in lines[start_index:header_end_index]:
        stripped = line.strip()
        if stripped.startswith(f"{field_name}:"):
            return stripped.split(":", maxsplit=1)[1].strip()
    return None


def _parse_datetime_or_min(value: str | None) -> datetime:
    if not value:
        return datetime.min.replace(tzinfo=UTC)
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _find_compatible_cluster(
    clusters: list[list[dict[str, Any]]],
    row: dict[str, Any],
) -> list[dict[str, Any]] | None:
    row_dt = _parse_datetime_or_min(row["acceptance_datetime_utc"])
    row_source_cik = str(row["source_cik"])
    row_counterparty = row.get("acquirer_cik") or row.get("counterparty_slug")

    for cluster in reversed(clusters):
        cluster_source_cik = str(cluster[0]["source_cik"])
        if cluster_source_cik != row_source_cik:
            continue
        cluster_latest_dt = max(
            _parse_datetime_or_min(item["acceptance_datetime_utc"]) for item in cluster
        )
        gap_days = abs((row_dt.date() - cluster_latest_dt.date()).days)
        cluster_counterparties = {
            item.get("acquirer_cik") or item.get("counterparty_slug")
            for item in cluster
            if item.get("acquirer_cik") or item.get("counterparty_slug")
        }
        if row_counterparty and row_counterparty in cluster_counterparties and gap_days <= 365:
            return cluster
        if not row_counterparty and gap_days <= 30:
            return cluster
        if row_counterparty and not cluster_counterparties and gap_days <= 30:
            return cluster
    return None


def _cluster_to_event_row(cluster: list[dict[str, Any]], event_index: int) -> dict[str, Any]:
    cluster = sorted(
        cluster, key=lambda row: _parse_datetime_or_min(row["acceptance_datetime_utc"])
    )
    anchor = cluster[0]
    disclosure_dt = _parse_datetime_or_min(anchor["acceptance_datetime_utc"])
    alignment = align_announcement_timestamp(disclosure_dt)

    counterparties = sorted(
        {
            row["acquirer_name"]
            for row in cluster
            if row.get("acquirer_name") and row.get("acquirer_name") != row["source_name"]
        }
    )
    counterparty_name = counterparties[0] if len(counterparties) == 1 else None
    counterparty_slug = (
        normalize_counterparty_name(counterparty_name) if counterparty_name else None
    )
    candidate_forms = sorted({str(row["form"]) for row in cluster})
    candidate_accessions = sorted({str(row["accession_number"]) for row in cluster})
    source_cik = str(anchor["source_cik"])
    event_id = (
        f"sec_mna_{source_cik}_{alignment.event_trading_date.isoformat()}_"
        f"{counterparty_slug or f'cluster{event_index:04d}'}"
    )

    requires_manual_review = (
        any(bool(row["requires_manual_review"]) for row in cluster)
        or len(counterparties) != 1
        or any(not row.get("counterparty_name") for row in cluster)
    )
    return {
        "event_id": event_id,
        "source_firm_id": anchor["source_firm_id"],
        "source_cik": source_cik,
        "source_name": anchor["source_name"],
        "source_ticker": anchor["source_ticker"],
        "source_underlying_symbol": anchor["source_underlying_symbol"],
        "target_firm_id": anchor["target_firm_id"],
        "target_cik": anchor["target_cik"],
        "target_name": anchor["target_name"],
        "target_ticker": anchor["target_ticker"],
        "target_underlying_symbol": anchor["target_underlying_symbol"],
        "acquirer_firm_id": anchor["acquirer_firm_id"] if len(counterparties) == 1 else None,
        "acquirer_cik": anchor["acquirer_cik"] if len(counterparties) == 1 else None,
        "acquirer_name": counterparty_name,
        "acquirer_ticker": anchor["acquirer_ticker"] if len(counterparties) == 1 else None,
        "acquirer_underlying_symbol": (
            anchor["acquirer_underlying_symbol"] if len(counterparties) == 1 else None
        ),
        "first_public_disclosure_dt": disclosure_dt.isoformat(),
        "first_public_disclosure_filing_date": anchor["filing_date"],
        "event_trading_date": alignment.event_trading_date.isoformat(),
        "pre_event_window_end": alignment.pre_event_window_end.isoformat(),
        "announcement_form": anchor["form"],
        "announcement_accession_number": anchor["accession_number"],
        "announcement_filing_url": anchor["raw_filing_url"],
        "deal_type": anchor["deal_type"],
        "counterparty_name": counterparty_name,
        "counterparty_slug": counterparty_slug,
        "source_resolution": anchor["source_resolution"],
        "target_resolution": anchor["target_resolution"],
        "acquirer_resolution": anchor["acquirer_resolution"],
        "candidate_filing_count": len(cluster),
        "candidate_forms": ";".join(candidate_forms),
        "candidate_accessions": ";".join(candidate_accessions),
        "max_match_score": max(int(row["mna_match_score"]) for row in cluster),
        "requires_manual_review": requires_manual_review,
        "has_conflicting_counterparties": len(counterparties) > 1,
        "has_conflicting_acquirers": len(counterparties) > 1,
        "cluster_start_dt": cluster[0]["acceptance_datetime_utc"],
        "cluster_end_dt": cluster[-1]["acceptance_datetime_utc"],
    }


def _candidate_frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(
            schema={
                "matched_symbol": pl.String,
                "matched_cik": pl.Int64,
                "matched_company_name": pl.String,
                "matched_ticker": pl.String,
                "matched_exchange": pl.String,
                "accession_number": pl.String,
                "filing_date": pl.String,
                "acceptance_datetime_utc": pl.String,
                "form": pl.String,
                "items": pl.String,
                "primary_document": pl.String,
                "primary_doc_description": pl.String,
                "raw_filing_url": pl.String,
                "filer_name": pl.String,
                "filer_cik": pl.String,
                "filed_by_name": pl.String,
                "filed_by_cik": pl.String,
                "subject_company_name": pl.String,
                "subject_company_cik": pl.String,
                "source_firm_id": pl.String,
                "source_cik": pl.String,
                "source_name": pl.String,
                "source_ticker": pl.String,
                "source_underlying_symbol": pl.String,
                "source_resolution": pl.String,
                "source_has_option_data": pl.Boolean,
                "target_firm_id": pl.String,
                "target_cik": pl.String,
                "target_name": pl.String,
                "target_ticker": pl.String,
                "target_underlying_symbol": pl.String,
                "target_resolution": pl.String,
                "target_has_option_data": pl.Boolean,
                "acquirer_firm_id": pl.String,
                "acquirer_cik": pl.String,
                "acquirer_name": pl.String,
                "acquirer_ticker": pl.String,
                "acquirer_underlying_symbol": pl.String,
                "acquirer_resolution": pl.String,
                "acquirer_has_option_data": pl.Boolean,
                "counterparty_name": pl.String,
                "counterparty_slug": pl.String,
                "deal_type": pl.String,
                "mna_match_score": pl.Int64,
                "matched_keyword_count": pl.Int64,
                "matched_keywords": pl.String,
                "exclusion_hits": pl.String,
                "text_excerpt": pl.String,
                "is_mna_candidate": pl.Boolean,
                "is_target_side": pl.Boolean,
                "requires_manual_review": pl.Boolean,
            }
        )
    return pl.from_dicts(rows)


def _empty_historical_resolution_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "underlying_symbol": pl.String,
            "normalized_symbol": pl.String,
            "cik": pl.Int64,
            "matched_company_name": pl.String,
            "matched_ticker": pl.String,
            "matched_exchange": pl.String,
            "normalized_company_name": pl.String,
            "matched_company_slug": pl.String,
            "historical_resolution_source": pl.String,
            "historical_resolution_score": pl.Int64,
            "historical_resolution_hit_count": pl.Int64,
            "historical_resolution_accessions": pl.String,
        }
    )


def _empty_event_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "event_id": pl.String,
            "source_firm_id": pl.String,
            "source_cik": pl.String,
            "source_name": pl.String,
            "source_ticker": pl.String,
            "source_underlying_symbol": pl.String,
            "target_firm_id": pl.String,
            "target_cik": pl.String,
            "target_name": pl.String,
            "target_ticker": pl.String,
            "target_underlying_symbol": pl.String,
            "acquirer_firm_id": pl.String,
            "acquirer_cik": pl.String,
            "acquirer_name": pl.String,
            "acquirer_ticker": pl.String,
            "acquirer_underlying_symbol": pl.String,
            "first_public_disclosure_dt": pl.String,
            "first_public_disclosure_filing_date": pl.String,
            "event_trading_date": pl.String,
            "pre_event_window_end": pl.String,
            "announcement_form": pl.String,
            "announcement_accession_number": pl.String,
            "announcement_filing_url": pl.String,
            "deal_type": pl.String,
            "counterparty_name": pl.String,
            "counterparty_slug": pl.String,
            "source_resolution": pl.String,
            "target_resolution": pl.String,
            "acquirer_resolution": pl.String,
            "candidate_filing_count": pl.Int64,
            "candidate_forms": pl.String,
            "candidate_accessions": pl.String,
            "max_match_score": pl.Int64,
            "requires_manual_review": pl.Boolean,
            "has_conflicting_counterparties": pl.Boolean,
            "has_conflicting_acquirers": pl.Boolean,
            "cluster_start_dt": pl.String,
            "cluster_end_dt": pl.String,
        }
    )
