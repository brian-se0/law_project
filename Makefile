.PHONY: init ingest-options build-underlyings build-events build-linkages freeze-case build-buckets main-study paper test

PYTHON ?= python

init:
	$(PYTHON) -m pip install -e .[dev]

ingest-options:
	$(PYTHON) scripts/ingest_options.py --project-root .

build-underlyings:
	$(PYTHON) scripts/build_underlying_daily.py --project-root .

build-events:
	$(PYTHON) scripts/build_mna_event_universe.py --project-root .

build-linkages:
	$(PYTHON) scripts/build_linkages.py --project-root .

freeze-case:
	$(PYTHON) scripts/freeze_mdvn_case_event.py --project-root .

build-buckets:
	$(PYTHON) scripts/build_option_buckets.py --project-root .

main-study:
	$(PYTHON) scripts/run_mdvn_case_study.py --project-root .

paper:
	$(PYTHON) scripts/make_mdvn_outputs.py --project-root .

test:
	$(PYTHON) -m pytest -q
