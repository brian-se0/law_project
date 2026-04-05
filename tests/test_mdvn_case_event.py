from __future__ import annotations

import polars as pl

from shadow_trading.case_study import freeze_case_event

from case_study_fixtures import seed_case_study_inputs


def test_freeze_case_event_writes_one_row_only(tmp_path) -> None:
    config = seed_case_study_inputs(tmp_path, include_frozen_event=False)

    artifacts = freeze_case_event(config, overwrite=True)

    frozen = pl.read_parquet(artifacts.case_event_file)
    assert frozen.height == 1
    row = frozen.row(0, named=True)
    assert row["case_id"] == "mdvn_panuwat_2016"
    assert row["source_symbol"] == "MDVN"
    assert row["target_gvkey"] == "1111"
    assert row["event_id"] == "sec_mna_mdvn_2016-08-22_pfizer"
