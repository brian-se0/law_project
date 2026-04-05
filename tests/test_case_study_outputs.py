from __future__ import annotations

from shadow_trading.case_study import run_case_study
from shadow_trading.plots import make_case_study_outputs

from case_study_fixtures import seed_case_study_inputs


def test_case_study_outputs_write_memos_and_escape_series_ids(tmp_path) -> None:
    config = seed_case_study_inputs(tmp_path, include_frozen_event=True)

    run_case_study(config, overwrite=True)
    artifacts = make_case_study_outputs(config)

    assert artifacts.memo_paths["watchlist_compliance"].exists()
    assert artifacts.memo_paths["limitations"].exists()

    exact_contract_inventory = artifacts.table_paths["exact_contracts"].read_text(encoding="utf-8")
    assert r"INCY\|INCY\|2016-09-16\|80.0\|C" in exact_contract_inventory
