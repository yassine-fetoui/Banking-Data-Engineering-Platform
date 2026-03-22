.PHONY: help install test lint format docker-up docker-down \
        run-bronze run-silver run-gold \
        terraform-init terraform-plan terraform-apply \
        dbt-run dbt-test dbt-docs

PYTHON  := python3.11
VENV    := .venv
ENV     ?= local

help:
	@awk 'BEGIN {FS = ":.*##"; printf "\n\033[1mUsage:\033[0m\n  make \033[36m<target>\033[0m [ENV=local|dev|prod]\n\n\033[1mTargets:\033[0m\n"} \
	/^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-25s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

## ── Setup ─────────────────────────────────────────────────────────────────────
install: ## Install all Python dependencies + pre-commit hooks
	$(PYTHON) -m venv $(VENV)
	$(VENV)/bin/pip install --upgrade pip
	$(VENV)/bin/pip install -e ".[dev]"
	$(VENV)/bin/pre-commit install

## ── Code Quality ──────────────────────────────────────────────────────────────
lint: ## Lint (ruff + black check + mypy)
	$(VENV)/bin/ruff check spark/ airflow/ kafka/ tests/
	$(VENV)/bin/black --check spark/ airflow/ kafka/ tests/
	$(VENV)/bin/mypy spark/ --ignore-missing-imports

format: ## Auto-format code
	$(VENV)/bin/black spark/ airflow/ kafka/ tests/
	$(VENV)/bin/ruff check --fix spark/ airflow/ kafka/ tests/

## ── Testing ───────────────────────────────────────────────────────────────────
test: ## Run all unit tests
	$(VENV)/bin/pytest tests/unit/ -v --cov=spark --cov-report=term-missing

test-integration: ## Run integration tests (requires local Docker stack)
	$(VENV)/bin/pytest tests/integration/ -v

## ── Docker / Local Stack ─────────────────────────────────────────────────────
docker-up: ## Start Kafka + Airflow + Spark locally
	docker compose up -d
	@echo "Airflow → http://localhost:8080 | Kafka UI → http://localhost:8090"

docker-down: ## Stop local stack
	docker compose down -v

## ── Spark Pipeline ────────────────────────────────────────────────────────────
run-bronze: ## Run Bronze ingestion (ENV=local|dev|prod)
	$(VENV)/bin/python -m spark.bronze.ingest_transactions --env $(ENV)
	$(VENV)/bin/python -m spark.bronze.ingest_customers    --env $(ENV)
	$(VENV)/bin/python -m spark.bronze.ingest_accounts     --env $(ENV)

run-silver: ## Run Silver cleansing & enrichment
	$(VENV)/bin/python -m spark.silver.cleanse_transactions --env $(ENV)
	$(VENV)/bin/python -m spark.silver.scd2_customers       --env $(ENV)

run-gold: ## Run Gold aggregations
	$(VENV)/bin/python -m spark.gold.daily_pnl       --env $(ENV)
	$(VENV)/bin/python -m spark.gold.aml_risk_scores --env $(ENV)

## ── dbt ───────────────────────────────────────────────────────────────────────
dbt-run: ## Run all dbt models
	cd dbt && dbt deps && dbt run --profiles-dir .

dbt-test: ## Run dbt tests
	cd dbt && dbt test --profiles-dir .

dbt-docs: ## Generate and serve dbt docs
	cd dbt && dbt docs generate && dbt docs serve

## ── Terraform ─────────────────────────────────────────────────────────────────
terraform-init: ## Init Terraform for ENV
	cd terraform/environments/$(ENV) && terraform init

terraform-plan: ## Plan Terraform for ENV
	cd terraform/environments/$(ENV) && terraform plan

terraform-apply: ## Apply Terraform for ENV
	cd terraform/environments/$(ENV) && terraform apply

## ── Data Quality ──────────────────────────────────────────────────────────────
ge-run: ## Run Great Expectations checkpoints
	$(VENV)/bin/great_expectations checkpoint run bronze_transactions
	$(VENV)/bin/great_expectations checkpoint run silver_customers
