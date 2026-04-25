# MarketMind Intelligence Platform - Makefile
# Common commands for development workflow

.PHONY: help setup setup-dev test test-unit test-integration test-regression test-performance lint lint-fix airflow-init clean

help:
	@echo "MarketMind Intelligence Platform - Available Commands"
	@echo "======================================================"
	@echo "setup              - Install Python dependencies"
	@echo "setup-dev          - Install with development dependencies"
	@echo "test               - Run all tests"
	@echo "test-unit          - Run unit tests only"
	@echo "test-integration   - Run integration tests only"
	@echo "test-regression    - Run regression tests only"
	@echo "test-performance   - Run performance tests only"
	@echo "lint               - Run ruff linter"
	@echo "lint-fix           - Auto-fix ruff errors"
	@echo "airflow-init       - Initialize Airflow database"
	@echo "clean              - Remove cache and artifacts"

setup:
	pip install -r requirements.txt

setup-dev:
	pip install -e .[dev]

test:
	pytest tests/ -v

test-unit:
	pytest tests/unit/ -v --cov=code --cov-report=term-missing --cov-report=html

test-integration:
	pytest tests/integration/ -v

test-regression:
	pytest tests/regression/ -v

test-performance:
	pytest tests/performance/ -v

lint:
	ruff check . --exclude airflow/dags/

lint-fix:
	ruff check --fix .
	ruff check --fix --unsafe-fixes .

airflow-init:
	export AIRFLOW_HOME=$$(pwd)/airflow && airflow db init

clean:
	rm -rf .coverage coverage.xml htmlcov/
	rm -rf .pytest_cache
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
