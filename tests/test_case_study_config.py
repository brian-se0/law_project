from __future__ import annotations

from case_study_fixtures import build_case_study_config


def test_case_study_config_loads_mdvn_block(tmp_path) -> None:
    config = build_case_study_config(tmp_path)

    assert config.case_study.case_id == "mdvn_panuwat_2016"
    assert config.case_study.source_symbol == "MDVN"
    assert config.case_study.primary_related_symbol == "INCY"
    assert config.case_study.windows.terminal_case == (-2, -1)


def test_exact_contracts_parse_to_stable_series_ids(tmp_path) -> None:
    config = build_case_study_config(tmp_path)

    series_ids = [contract.series_id for contract in config.case_study.exact_contracts]

    assert series_ids == ["INCY|INCY|2016-09-16|80.0|C"]
