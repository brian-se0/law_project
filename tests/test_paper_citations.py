from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_paper_contains_expected_cite_keys() -> None:
    paper_text = (PROJECT_ROOT / "paper" / "mdvn_panuwat_case_study.md").read_text(encoding="utf-8")

    expected_keys = [
        "sec2021panuwatcomplaint",
        "sec2024panuwatverdict",
        "sec2024panuwatfinaljudgment",
        "cboe_option_eod_summary",
        "augustin2019informed",
        "deuskar2025peerstocks",
        "du2025rivals",
        "hoberg2016text",
        "fresard2020innovation",
        "cao2024informativeness",
    ]
    for key in expected_keys:
        assert f"@{key}" in paper_text


def test_literature_review_no_longer_treats_deuskar_as_working_paper() -> None:
    literature_review = (PROJECT_ROOT / "docs" / "literature_review.md").read_text(encoding="utf-8")

    assert "best treated as a working-paper citation" not in literature_review
    assert "2024 working paper, accepted at *Management Science*" not in literature_review
    assert "Management Science* 71(3):2390-2412" in literature_review


def test_references_include_legal_and_vendor_keys() -> None:
    references = (PROJECT_ROOT / "references.bib").read_text(encoding="utf-8")

    expected_keys = [
        "sec2021panuwatcomplaint",
        "sec2024panuwatverdict",
        "sec2024panuwatfinaljudgment",
        "cboe_option_eod_summary",
    ]
    for key in expected_keys:
        assert f"{{{key}," in references
