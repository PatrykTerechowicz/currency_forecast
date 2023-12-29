.DEFAULT_GOAL := run_web

PYTHON = python3

.PHONY: clean

init: requirements.txt
	pip install -r requirements.txt

test: init
	pytest tests

coverage_test: init
	coverage run --source=nbp_prophet --branch -m pytest tests 
	coverage report

clean:
	find . -type d -name __pycache__ -exec rm -r {} \+
	rm -rf htmlcov
	rm -f .coverage
	rm -rf tests/data.parquet

run_web: init
	${PYTHON} nbp_prophet/web_ui/app.py
	
push: clean
	git push -u origin main