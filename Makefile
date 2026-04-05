.PHONY: init ingest-options build-underlyings build-events build-linkages build-buckets replicate main-study policy-text paper test

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

build-buckets:
	@echo "Not implemented yet. Next planned script: scripts/build_option_buckets.py"

replicate:
	@echo "Not implemented yet. Next planned script: scripts/run_replication.py"

main-study:
	@echo "Not implemented yet. Next planned script: scripts/run_shadow_study.py"

policy-text:
	@echo "Not implemented yet. Next planned script: scripts/parse_policies.py"

paper:
	@echo "Not implemented yet. Next planned script: scripts/make_paper_outputs.py"

test:
	$(PYTHON) -m pytest -q
