from __future__ import annotations

import json
from datetime import date

import polars as pl

from shadow_trading.linkages import (
    build_gvkey_underlying_bridge,
    build_linkage_tables,
    discover_linkage_files,
)


def test_discover_linkage_files_ignores_readmes(tmp_path) -> None:
    raw_linkages_dir = tmp_path / "linkages"
    horizontal_dir = raw_linkages_dir / "tnic3_data"
    vertical_dir = raw_linkages_dir / "VertNetwork_10gran"
    horizontal_dir.mkdir(parents=True)
    vertical_dir.mkdir(parents=True)

    (horizontal_dir / "Readme_tnic3.txt").write_text("notes", encoding="utf-8")
    (horizontal_dir / "tnic3_data.txt").write_text(
        "year\tgvkey1\tgvkey2\tscore\n2020\t1001\t2002\t0.80\n",
        encoding="utf-8",
    )
    (vertical_dir / "Readme_VertNetwork_10gran.txt").write_text("notes", encoding="utf-8")
    (vertical_dir / "VertNetwork_10gran.txt").write_text(
        "year\tgvkey1\tgvkey2\tvertscore\n2020\t1001\t3003\t0.55\n",
        encoding="utf-8",
    )

    discovered = discover_linkage_files(raw_linkages_dir)

    assert discovered["horizontal_tnic"].name == "tnic3_data.txt"
    assert discovered["vertical_vtnic"].name == "VertNetwork_10gran.txt"


def test_build_gvkey_underlying_bridge_uses_event_and_current_sec_evidence(tmp_path) -> None:
    cache_root = tmp_path / "sec"
    cache_root.mkdir()
    (cache_root / "company_tickers_exchange.json").write_text(
        json.dumps(
            {
                "fields": ["cik", "name", "ticker", "exchange"],
                "data": [
                    [2, "Peer Holdings Inc.", "PEER", "Nasdaq"],
                    [3, "Supply Co.", "SUPP", "NYSE"],
                ],
            }
        ),
        encoding="utf-8",
    )

    seed_path = tmp_path / "linkages" / "gvkey_ciks_seed.csv"
    seed_path.parent.mkdir()
    seed_path.write_text(
        "\n".join(
            [
                "gvkey,iid,cik,first_date,last_date",
                "1001,01,0000000001,2010-01-01,2022-12-31",
                "2002,01,0000000002,2010-01-01,2022-12-31",
                "3003,01,0000000003,2010-01-01,2022-12-31",
            ]
        ),
        encoding="utf-8",
    )

    underlyings = pl.DataFrame(
        {
            "quote_date": [date(2021, 3, 9), date(2021, 3, 9), date(2021, 3, 9)],
            "underlying_symbol": ["EXM", "PEER", "SUPP"],
        }
    )
    events = pl.DataFrame(
        {
            "event_id": ["sec_evt_1"],
            "event_trading_date": ["2021-03-09"],
            "first_public_disclosure_dt": ["2021-03-08T22:15:00+00:00"],
            "source_firm_id": ["EXM"],
            "source_cik": ["0000000001"],
            "source_name": ["Example Corp"],
            "source_ticker": ["EXM"],
            "source_underlying_symbol": ["EXM"],
            "target_cik": [None],
            "target_name": [None],
            "target_ticker": [None],
            "target_underlying_symbol": [None],
            "acquirer_cik": [None],
            "acquirer_name": [None],
            "acquirer_ticker": [None],
            "acquirer_underlying_symbol": [None],
        }
    )

    bridge, metadata = build_gvkey_underlying_bridge(
        underlyings=underlyings,
        events=events,
        cache_root=cache_root,
        user_agent="law_project_research research@example.com",
        request_spacing_seconds=0.0,
        seed_path=seed_path,
    )

    assert set(bridge.get_column("underlying_symbol").to_list()) == {"EXM", "PEER", "SUPP"}
    assert metadata["bridge_row_count"] == 3
    exm = bridge.filter(pl.col("underlying_symbol") == "EXM").row(0, named=True)
    assert exm["gvkey"] == "1001"
    assert exm["bridge_method"] == "sec_event_evidence_plus_gvkey_cik_seed"
    peer = bridge.filter(pl.col("underlying_symbol") == "PEER").row(0, named=True)
    assert peer["bridge_method"] == "current_sec_ticker_plus_gvkey_cik_seed"


def test_build_linkage_tables_generates_lagged_links_and_controls_from_gvkey_files(
    tmp_path,
) -> None:
    raw_linkages_dir = tmp_path / "linkages"
    horizontal_dir = raw_linkages_dir / "tnic3_data"
    vertical_dir = raw_linkages_dir / "VertNetwork_10gran"
    horizontal_dir.mkdir(parents=True)
    vertical_dir.mkdir(parents=True)

    (horizontal_dir / "Readme_tnic3.txt").write_text("notes", encoding="utf-8")
    (horizontal_dir / "tnic3_data.txt").write_text(
        "\n".join(
            [
                "year\tgvkey1\tgvkey2\tscore",
                "2020\t1001\t2002\t0.80",
                "2020\t2002\t4004\t0.40",
            ]
        ),
        encoding="utf-8",
    )
    (vertical_dir / "Readme_VertNetwork_10gran.txt").write_text("notes", encoding="utf-8")
    (vertical_dir / "VertNetwork_10gran.txt").write_text(
        "\n".join(
            [
                "year\tgvkey1\tgvkey2\tvertscore",
                "2020\t1001\t3003\t0.55",
            ]
        ),
        encoding="utf-8",
    )

    events = pl.DataFrame(
        {
            "event_trading_date": ["2021-03-09"],
            "source_firm_id": ["EXM"],
            "source_ticker": ["EXM"],
        }
    )
    underlyings = pl.DataFrame(
        {
            "quote_date": [
                date(2021, 3, 9),
                date(2021, 3, 9),
                date(2021, 3, 9),
                date(2021, 3, 9),
            ],
            "underlying_symbol": ["EXM", "PEER", "SUPP", "CTRL"],
        }
    )
    bridge = pl.DataFrame(
        {
            "gvkey": ["1001", "2002", "3003", "4004"],
            "iid": ["01", "01", "01", "01"],
            "cik": ["0000000001", "0000000002", "0000000003", "0000000004"],
            "event_year": [2021, 2021, 2021, 2021],
            "underlying_symbol": ["EXM", "PEER", "SUPP", "CTRL"],
            "firm_id": ["EXM", "PEER", "SUPP", "CTRL"],
            "observed_start_date": [date(2021, 3, 9)] * 4,
            "observed_end_date": [date(2021, 3, 9)] * 4,
            "option_obs_count": [1, 1, 1, 1],
            "seed_first_date": [date(2010, 1, 1)] * 4,
            "seed_last_date": [date(2022, 12, 31)] * 4,
            "issuer_ticker": ["EXM", "PEER", "SUPP", "CTRL"],
            "issuer_name": ["Example", "Peer", "Supplier", "Control"],
            "issuer_sources": [
                "sec_event_source",
                "current_sec_company_tickers",
                "current_sec_company_tickers",
                "current_sec_company_tickers",
            ],
            "evidence_event_ids": ["sec_evt_1", None, None, None],
            "bridge_method": [
                "sec_event_evidence_plus_gvkey_cik_seed",
                "current_sec_ticker_plus_gvkey_cik_seed",
                "current_sec_ticker_plus_gvkey_cik_seed",
                "current_sec_ticker_plus_gvkey_cik_seed",
            ],
            "bridge_confidence": ["high", "high", "high", "high"],
        }
    )

    linkages, controls, metadata = build_linkage_tables(
        events=events,
        underlyings=underlyings,
        raw_linkages_dir=raw_linkages_dir,
        gvkey_underlying_bridge=bridge,
    )

    assert metadata["source_event_gvkey_count"] == 1
    assert metadata["bridge_row_count"] == 4
    assert set(linkages.get_column("link_type").to_list()) == {"horizontal_tnic", "vertical_vtnic"}
    assert (
        linkages.filter(
            (pl.col("link_type") == "horizontal_tnic") & (pl.col("linked_firm_id") == "PEER")
        ).height
        == 1
    )
    assert (
        linkages.filter(
            (pl.col("link_type") == "vertical_vtnic") & (pl.col("linked_firm_id") == "SUPP")
        ).height
        == 1
    )
    assert linkages.get_column("link_rank").to_list() == [1, 1]

    horizontal_controls = controls.filter(pl.col("link_type") == "horizontal_tnic")
    vertical_controls = controls.filter(pl.col("link_type") == "vertical_vtnic")

    assert set(horizontal_controls.get_column("control_firm_id").to_list()) == {"CTRL", "SUPP"}
    assert set(vertical_controls.get_column("control_firm_id").to_list()) == {"CTRL", "PEER"}
