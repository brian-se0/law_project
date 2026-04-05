from __future__ import annotations

from shadow_trading.case_study import build_case_buckets, build_case_study_paths

from case_study_fixtures import seed_case_study_inputs


def test_build_case_buckets_extracts_exact_contracts(tmp_path) -> None:
    config = seed_case_study_inputs(tmp_path, include_frozen_event=True)

    artifacts = build_case_buckets(config, overwrite=True)

    exact_contracts = build_case_study_paths(config).exact_contracts_file
    assert exact_contracts.exists()
    assert artifacts.exact_contract_row_count > 0
