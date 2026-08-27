.PHONY: test test-local reinit-venv reinit-venv-local sync sync-local deploy-dev deploy-prod smoke-test-dev

# `local` and `databricks` never share a venv: databricks-connect vendors its
# own `pyspark` that collides file-for-file with the real pyspark `local`
# installs, so adding/removing one package in place can corrupt the other.
# `local` gets its own venv (.venv-local); `databricks` uses the normal .venv.

reinit-venv:
	rm -rf .venv && $(MAKE) sync

reinit-venv-local:
	rm -rf .venv-local && $(MAKE) sync-local

sync:
	uv sync --exact

sync-local:
	UV_PROJECT_ENVIRONMENT=.venv-local uv sync --exact --no-default-groups --group local

test:
	uv run --exact pytest tests/unit $(ARGS)

test-local:
	UV_PROJECT_ENVIRONMENT=.venv-local uv run --no-default-groups --group local pytest tests/unit $(ARGS)

smoke-test-dev: deploy-dev
	uv run --exact pytest tests/integration -m integration $(ARGS) 

deploy-dev:
	databricks bundle validate -t dev && \
	databricks bundle deploy -t dev

deploy-prod:
	databricks bundle validate -t prod && \
	databricks bundle deploy -t prod