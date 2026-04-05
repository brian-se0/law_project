from __future__ import annotations

import pytest

from shadow_trading.case_study import run_case_study

from case_study_fixtures import seed_case_study_inputs


def test_case_study_runner_fails_loudly_if_frozen_case_event_is_missing(tmp_path) -> None:
    config = seed_case_study_inputs(tmp_path, include_frozen_event=False)

    with pytest.raises(FileNotFoundError, match="freeze_mdvn_case_event.py"):
        run_case_study(config, overwrite=True)
