from __future__ import annotations

from zipfile import ZipFile

from shadow_trading.case_study import run_case_study
from shadow_trading.plots import make_case_study_outputs
from shadow_trading.release import package_assessment_bundle

from case_study_fixtures import seed_case_study_inputs, seed_release_bundle_support_files


def test_package_assessment_bundle_includes_exact_contract_summary_and_commit_sha(
    tmp_path,
) -> None:
    config = seed_case_study_inputs(tmp_path, include_frozen_event=True)
    seed_release_bundle_support_files(tmp_path)

    run_case_study(config, overwrite=True)
    make_case_study_outputs(config)

    commit_sha = "deadbeefcafebabe1234567890abcdef12345678"
    bundle_path = package_assessment_bundle(
        config=config,
        project_root=tmp_path,
        commit_sha=commit_sha,
    )

    with ZipFile(bundle_path) as archive:
        bundle_entries = archive.namelist()
        package_readme = archive.read("PACKAGE_README.md").decode("utf-8")

    assert "outputs/tables/mdvn_exact_contract_window_summary.md" in bundle_entries
    assert commit_sha in package_readme
