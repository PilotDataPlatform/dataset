.PHONY: help test

.DEFAULT: help

help:
	@echo "make test"
	@echo "    run tests"

test:
	PYTHONPATH=. poetry run pytest -s --cov=app --cov-report term-missing --disable-warnings
