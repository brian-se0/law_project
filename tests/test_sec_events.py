from __future__ import annotations

from pathlib import Path

import polars as pl

from shadow_trading.sec_party import build_company_lookup_index, resolve_deal_parties
from shadow_trading.sec_events import (
    _historical_company_candidates_from_search_hit,
    build_historical_symbol_search_queries,
    build_sec_event_universe,
    classify_filing_text,
    company_name_matches_context,
    company_name_matches_search_query,
    extract_symbol_context_company_names,
    extract_filing_header,
    filing_text_to_plain_text,
    historical_company_candidates_are_ambiguous,
    normalize_counterparty_name,
    normalize_full_text_search_forms,
    parse_sec_company_search_direct_candidates,
    parse_display_name_candidates,
    resolve_counterparty_name,
    resolve_source_company,
    search_sec_company_candidates_by_name,
    select_historical_company_candidate,
)

RAW_TENDER_FILING = """<SEC-DOCUMENT>0001104659-24-041703.txt : 20240401
<SEC-HEADER>0001104659-24-041703.hdr.sgml : 20240401
<ACCEPTANCE-DATETIME>20240401130743
ACCESSION NUMBER:\t\t0001104659-24-041703
CONFORMED SUBMISSION TYPE:\tSC TO-T
SUBJECT COMPANY:

\tCOMPANY DATA:
\t\tCOMPANY CONFORMED NAME:\t\t\tInland Real Estate Income Trust, Inc.
\t\tCENTRAL INDEX KEY:\t\t\t0001528985

FILED BY:

\tCOMPANY DATA:
\t\tCOMPANY CONFORMED NAME:\t\t\tComrit Investments 1, LP
\t\tCENTRAL INDEX KEY:\t\t\t0001726993
</SEC-HEADER>
<DOCUMENT>
<TYPE>SC TO-T
<TEXT>
Tender Offer Statement. Offer to purchase all outstanding shares of Inland Real Estate Income Trust, Inc.
</TEXT>
</DOCUMENT>
"""

RAW_MERGER_8K = """<SEC-DOCUMENT>0000000000-24-000001.txt : 20240401
<SEC-HEADER>0000000000-24-000001.hdr.sgml : 20240401
<ACCEPTANCE-DATETIME>20240401170500
ACCESSION NUMBER:\t\t0000000000-24-000001
CONFORMED SUBMISSION TYPE:\t8-K
FILER:

\tCOMPANY DATA:
\t\tCOMPANY CONFORMED NAME:\t\t\tExample Target, Inc.
\t\tCENTRAL INDEX KEY:\t\t\t0001234567
</SEC-HEADER>
<DOCUMENT>
<TYPE>8-K
<TEXT>
On April 1, 2024, Example Target, Inc. entered into an Agreement and Plan of Merger with Acquirer Holdings, Inc.
Pursuant to the merger agreement, Example Target, Inc. will be acquired by Acquirer Holdings, Inc.
</TEXT>
</DOCUMENT>
"""

RAW_SUBJECT_COMPANY_425 = """<SEC-DOCUMENT>0000002488-21-000001.txt : 20210114
<SEC-HEADER>0000002488-21-000001.hdr.sgml : 20210114
<ACCEPTANCE-DATETIME>20210114100500
ACCESSION NUMBER:\t\t0000002488-21-000001
CONFORMED SUBMISSION TYPE:\t425
SUBJECT COMPANY:

\tCOMPANY DATA:
\t\tCOMPANY CONFORMED NAME:\t\t\tXilinx, Inc.
\t\tCENTRAL INDEX KEY:\t\t\t0000743988

FILED BY:

\tCOMPANY DATA:
\t\tCOMPANY CONFORMED NAME:\t\t\tAdvanced Micro Devices, Inc.
\t\tCENTRAL INDEX KEY:\t\t\t0000002488
</SEC-HEADER>
<DOCUMENT>
<TYPE>425
<TEXT>
Advanced Micro Devices, Inc. announced that it will acquire Xilinx, Inc. (NASDAQ: XLNX) in an all-stock transaction.
</TEXT>
</DOCUMENT>
"""

SEC_COMPANY_SEARCH_DIRECT_HTML = """<!DOCTYPE html>
<html>
<body>
<div class="companyInfo">
<span class="companyName">Teledyne FLIR, LLC <acronym title="Central Index Key">CIK</acronym>#: <a href="/cgi-bin/browse-edgar?action=getcompany&amp;CIK=0000354908&amp;owner=exclude&amp;count=40">0000354908 (see all company filings)</a></span>
<p class="identInfo">formerly: FLIR SYSTEMS INC (filings through 2021-05-14)</p>
</div>
</body>
</html>
"""


def test_extract_filing_header_reads_subject_and_filed_by_sections() -> None:
    header = extract_filing_header(RAW_TENDER_FILING)

    assert header["acceptance_datetime"] == "2024-04-01T13:07:43+00:00"
    assert header["subject_company_name"] == "Inland Real Estate Income Trust, Inc."
    assert header["subject_company_cik"] == "0001528985"
    assert header["filed_by_name"] == "Comrit Investments 1, LP"
    assert header["filed_by_cik"] == "0001726993"


def test_classify_filing_text_identifies_mna_keywords() -> None:
    plain_text = filing_text_to_plain_text(RAW_MERGER_8K)
    classification = classify_filing_text(
        form="8-K",
        items="1.01,8.01,9.01",
        primary_doc_description="8-K",
        plain_text=plain_text,
    )

    assert classification["is_mna_candidate"] is True
    assert classification["is_target_side"] is True
    assert classification["deal_type"] == "merger"
    assert "agreement_and_plan_of_merger" in classification["matched_keywords"]


def test_resolve_counterparty_prefers_filed_by_for_subject_company_forms() -> None:
    header = extract_filing_header(RAW_TENDER_FILING)
    plain_text = filing_text_to_plain_text(RAW_TENDER_FILING)

    source_cik, source_name, source_resolution = resolve_source_company(
        matched_cik=1528985,
        matched_name="Inland Real Estate Income Trust, Inc.",
        matched_ticker="INRE",
        form="SC TO-T",
        header=header,
    )
    counterparty = resolve_counterparty_name(
        header=header,
        source_name=source_name,
        source_resolution=source_resolution,
        plain_text=plain_text,
    )

    assert source_cik == "0001528985"
    assert source_resolution == "subject_company"
    assert counterparty == "Comrit Investments 1, LP"


def test_extract_symbol_context_company_names_reads_exchange_ticker_mentions() -> None:
    plain_text = filing_text_to_plain_text(RAW_SUBJECT_COMPANY_425)

    names = extract_symbol_context_company_names(plain_text, "XLNX")

    assert "Xilinx, Inc" in names
    assert company_name_matches_context("Xilinx, Inc.", names) is True


def test_extract_symbol_context_company_names_reads_trading_symbol_sentences() -> None:
    plain_text = filing_text_to_plain_text("""
        <DOCUMENT>
        <TEXT>
        Xilinx common stock is traded on Nasdaq under the symbol "XLNX."
        </TEXT>
        </DOCUMENT>
        """)

    names = extract_symbol_context_company_names(plain_text, "XLNX")

    assert "Xilinx" in names


def test_parse_display_name_candidates_uses_exact_ticker_matches() -> None:
    candidates = parse_display_name_candidates(
        ["COHERENT CORP.  (COHR)  (CIK 0000820318)", "COHR Inc.  (CIK 0001594178)"],
        "COHR",
    )

    assert candidates == [
        {
            "cik": "0000820318",
            "name": "COHERENT CORP.",
            "match_source": "historical_display_name_ticker",
            "support_score": 95,
        }
    ]


def test_parse_display_name_candidates_accepts_single_targeted_match() -> None:
    candidates = parse_display_name_candidates(
        ["VARIAN MEDICAL SYSTEMS INC  (CIK 0000203527)"],
        "VAR",
        allow_single_name_match=True,
    )

    assert candidates == [
        {
            "cik": "0000203527",
            "name": "VARIAN MEDICAL SYSTEMS INC",
            "match_source": "historical_display_name_single_match",
            "support_score": 93,
        }
    ]


def test_parse_display_name_candidates_uses_symbol_name_hints_for_targeted_queries() -> None:
    candidates = parse_display_name_candidates(
        [
            "FLIR SYSTEMS INC  (CIK 0000354908)",
            "TELEDYNE TECHNOLOGIES INC  (TDY)  (CIK 0001094285)",
        ],
        "FLIR",
        allow_symbol_name_match=True,
    )

    assert candidates == [
        {
            "cik": "0000354908",
            "name": "FLIR SYSTEMS INC",
            "match_source": "historical_display_name_symbol_hint",
            "support_score": 94,
        }
    ]


def test_historical_company_candidates_prefer_subject_company_when_symbol_context_matches() -> None:
    class FakeClient:
        cache_root = Path(".")

        def fetch_text(self, url: str, cache_path) -> str:  # noqa: ANN001
            return RAW_SUBJECT_COMPANY_425

    client = FakeClient()
    hit = {
        "_id": "0000002488-21-000001:amd-425.htm",
        "_source": {
            "adsh": "0000002488-21-000001",
            "ciks": ["2488"],
            "display_names": ["ADVANCED MICRO DEVICES INC  (AMD)  (CIK 0000002488)"],
        },
    }

    candidates = _historical_company_candidates_from_search_hit(
        client=client,
        symbol="XLNX",
        hit=hit,
    )

    assert candidates[0]["cik"] == "0000743988"
    assert candidates[0]["matched_company_name"] == "Xilinx, Inc."
    assert candidates[0]["match_source"] == "historical_subject_company"
    assert candidates[0]["support_score"] == 100


def test_parse_sec_company_search_direct_candidates_reads_current_and_former_names() -> None:
    candidates = parse_sec_company_search_direct_candidates(
        html_text=SEC_COMPANY_SEARCH_DIRECT_HTML,
        company_name="FLIR Systems, Inc.",
    )

    assert candidates == [
        {
            "cik": "0000354908",
            "matched_company_name": "FLIR SYSTEMS INC",
            "match_source": "historical_company_search_former_name",
            "support_score": 94,
        }
    ]


def test_company_name_matches_search_query_requires_exact_or_prefix_match() -> None:
    assert company_name_matches_search_query("FLIR Systems, Inc.", "FLIR Systems, Inc.") is True
    assert company_name_matches_search_query("Teledyne Technologies Inc", "Teledyne") is False
    assert company_name_matches_search_query("LAND O LAKES INC", "Land") is False


def test_search_sec_company_candidates_by_name_uses_sec_company_page() -> None:
    class FakeClient:
        def __init__(self) -> None:
            self.cache_root = Path(".")

        def fetch_text(self, url: str, cache_path) -> str:  # noqa: ANN001
            assert "company=FLIR+Systems%2C+Inc" in url
            return SEC_COMPANY_SEARCH_DIRECT_HTML

    candidates = search_sec_company_candidates_by_name(
        client=FakeClient(),
        company_name="FLIR Systems, Inc.",
    )

    assert candidates == [
        {
            "cik": "0000354908",
            "matched_company_name": "FLIR SYSTEMS INC",
            "match_source": "historical_company_search_former_name",
            "support_score": 94,
        }
    ]


def test_build_historical_symbol_search_queries_adds_targeted_queries() -> None:
    queries = build_historical_symbol_search_queries("VAR")

    assert queries[0] == {"cache_label": "trading_symbol", "query": '"symbol VAR"'}
    assert {"cache_label": "exchange_nyse", "query": '"NYSE: VAR"'} in queries
    assert queries[-1] == {"cache_label": "symbol", "query": "VAR"}


def test_normalize_full_text_search_forms_drops_amendment_suffixes() -> None:
    forms = normalize_full_text_search_forms(["8-K/A", "S-4", "S-4/A", "14D9/A", "425"])

    assert forms == ["14D9", "425", "8-K", "S-4"]


def test_select_historical_company_candidate_prefers_repeated_strong_hits() -> None:
    best = select_historical_company_candidate(
        [
            {
                "cik": "0000743988",
                "matched_company_name": "XILINX INC",
                "match_source": "historical_filer_context",
                "support_score": 90,
                "supporting_accession_number": "0000743988-20-000047",
            },
            {
                "cik": "0000743988",
                "matched_company_name": "XILINX INC",
                "match_source": "historical_filer_context",
                "support_score": 90,
                "supporting_accession_number": "0000743988-20-000048",
            },
            {
                "cik": "0000002488",
                "matched_company_name": "ADVANCED MICRO DEVICES INC",
                "match_source": "historical_filer_context",
                "support_score": 90,
                "supporting_accession_number": "0000002488-20-000001",
            },
        ]
    )

    assert best is not None
    assert best["cik"] == "0000743988"
    assert best["support_hit_count"] == 2


def test_historical_company_candidates_are_ambiguous_when_runner_up_is_close() -> None:
    assert (
        historical_company_candidates_are_ambiguous(
            {
                "cik": "0000354908",
                "support_score": 94,
                "strong_match_count": 2,
                "support_hit_count": 3,
            },
            {
                "cik": "0001094285",
                "support_score": 94,
                "strong_match_count": 2,
                "support_hit_count": 3,
            },
        )
        is True
    )


def test_resolve_deal_parties_extracts_target_and_acquirer_matches() -> None:
    ticker_frame = pl.DataFrame(
        {
            "cik": [1234567, 7654321],
            "ticker": ["EXM", "ACQR"],
            "name": ["Example Target, Inc.", "Acquirer Holdings, Inc."],
            "exchange": ["NASDAQ", "NYSE"],
            "normalized_symbol": ["EXM", "ACQR"],
            "normalized_company_name": ["example_target", "acquirer"],
        }
    )
    matched_companies = pl.DataFrame(
        {
            "underlying_symbol": ["EXM"],
            "normalized_symbol": ["EXM"],
            "cik": [1234567],
            "matched_company_name": ["Example Target, Inc."],
            "matched_ticker": ["EXM"],
            "matched_exchange": ["NASDAQ"],
            "matched_company_slug": ["example_target"],
        }
    )
    lookups = build_company_lookup_index(
        ticker_frame=ticker_frame, matched_companies=matched_companies
    )
    header = extract_filing_header(RAW_MERGER_8K)
    plain_text = filing_text_to_plain_text(RAW_MERGER_8K)

    parties = resolve_deal_parties(
        matched_company=matched_companies.to_dicts()[0],
        form="8-K",
        header=header,
        plain_text=plain_text,
        company_lookups=lookups,
        subject_company_forms=(),
    )

    assert parties.source.firm_id == "EXM"
    assert parties.target.name == "Example Target, Inc."
    assert parties.acquirer is not None
    assert parties.acquirer.name == "Acquirer Holdings, Inc."
    assert parties.acquirer.ticker == "ACQR"


def test_build_sec_event_universe_deduplicates_same_deal_cluster() -> None:
    candidates = {
        "source_firm_id": ["EXM", "EXM"],
        "source_cik": ["0001234567", "0001234567"],
        "source_name": ["Example Target, Inc.", "Example Target, Inc."],
        "source_ticker": ["EXM", "EXM"],
        "source_underlying_symbol": ["EXM", "EXM"],
        "target_firm_id": ["EXM", "EXM"],
        "target_cik": ["0001234567", "0001234567"],
        "target_name": ["Example Target, Inc.", "Example Target, Inc."],
        "target_ticker": ["EXM", "EXM"],
        "target_underlying_symbol": ["EXM", "EXM"],
        "target_resolution": ["filer", "filer"],
        "acquirer_firm_id": ["ACQR", "ACQR"],
        "acquirer_cik": ["0007654321", "0007654321"],
        "acquirer_name": ["Acquirer Holdings, Inc.", "Acquirer Holdings, Inc."],
        "acquirer_ticker": ["ACQR", "ACQR"],
        "acquirer_underlying_symbol": ["ACQR", "ACQR"],
        "acquirer_resolution": ["text_be_acquired_by", "text_be_acquired_by"],
        "acceptance_datetime_utc": ["2024-04-01T20:05:00+00:00", "2024-04-15T12:00:00+00:00"],
        "filing_date": ["2024-04-01", "2024-04-15"],
        "form": ["8-K", "DEFM14A"],
        "accession_number": ["0000000000-24-000001", "0000000000-24-000002"],
        "raw_filing_url": ["https://example.com/1", "https://example.com/2"],
        "deal_type": ["merger", "merger"],
        "source_resolution": ["filer", "filer"],
        "counterparty_name": ["Acquirer Holdings, Inc.", "Acquirer Holdings, Inc."],
        "counterparty_slug": [
            normalize_counterparty_name("Acquirer Holdings, Inc."),
            normalize_counterparty_name("Acquirer Holdings, Inc."),
        ],
        "mna_match_score": [5, 4],
        "is_mna_candidate": [True, True],
        "is_target_side": [True, True],
        "requires_manual_review": [False, False],
    }

    events = build_sec_event_universe(pl.DataFrame(candidates))

    assert events.height == 1
    event = events.to_dicts()[0]
    assert event["source_firm_id"] == "EXM"
    assert event["source_cik"] == "0001234567"
    assert event["acquirer_name"] == "Acquirer Holdings, Inc."
    assert event["candidate_filing_count"] == 2
    assert event["event_trading_date"] == "2024-04-02"
    assert event["pre_event_window_end"] == "2024-04-01"
