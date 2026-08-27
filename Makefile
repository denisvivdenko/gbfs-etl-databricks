.PHONY: test test-local test-databricks

test: test-databricks

# `local` and `databricks` never share a venv: databricks-connect vendors its
# own `pyspark` that collides file-for-file with the real pyspark `local`
# installs, so adding/removing one package in place can corrupt the other.
# `local` gets its own venv (.venv-local); `databricks` uses the normal .venv.

sync:
	uv sync --exact

sync-local:
	UV_PROJECT_ENVIRONMENT=.venv-local uv sync --exact --no-default-groups --group local

test-local:
	UV_PROJECT_ENVIRONMENT=.venv-local uv run --no-default-groups --group local pytest $(ARGS)

test-databricks:
	uv run --exact pytest $(ARGS)

deploy-dev:
	databricks bundle validate -t dev && \
	databricks bundle deploy -t dev

deploy-prod:
	databricks bundle validate -t prod && \
	databricks bundle deploy -t prod 