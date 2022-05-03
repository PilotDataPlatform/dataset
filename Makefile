.PHONY: help test

.DEFAULT: help

help:
	@echo "make test"
	@echo "    run tests"

test:
	PYTHONPATH=. poetry run pytest -vvv --cov=app --cov-report term-missing --cov-report=xml --disable-warnings
