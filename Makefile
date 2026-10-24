PYTHON ?= python3

.PHONY: install run dry-run rules test coverage walkthrough inspect evidence benchmark data clean

install:            ## install runtime + test dependencies
	$(PYTHON) -m pip install -r requirements-dev.txt

run:                ## run the full pipeline (single command)
	$(PYTHON) -m pipeline

dry-run:            ## validate and report without writing to the database
	$(PYTHON) -m pipeline --dry-run

rules:              ## print the validation rule catalogue
	$(PYTHON) -m pipeline rules

test:               ## run the test suite
	$(PYTHON) -m pytest

coverage:           ## run tests with line coverage
	$(PYTHON) -m pytest --cov=pipeline --cov-report=term-missing

walkthrough:        ## run the stages one at a time and show intermediate output
	$(PYTHON) scripts/walkthrough.py

inspect:            ## summarize the database contents
	$(PYTHON) scripts/inspect_db.py

evidence:           ## demonstrate behaviour across repeated runs
	$(PYTHON) scripts/rerun_evidence.py

benchmark:          ## throughput on ~96k order rows
	$(PYTHON) scripts/benchmark.py --factor 30

data:               ## regenerate the sample input data (deterministic)
	$(PYTHON) scripts/generate_sample_data.py

clean:              ## remove all pipeline outputs
	rm -rf data/processed reports logs walkthrough_output .pytest_cache .coverage
