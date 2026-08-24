.PHONY: test test-local test-databricks

# Default: real end-to-end tests against Databricks Connect.
test: test-databricks

# `local` and `databricks` never share a venv: databricks-connect vendors its
# own `pyspark` that collides file-for-file with the real pyspark `local`
# installs, so adding/removing one package in place can corrupt the other.
# `local` gets its own venv (.venv-local); `databricks` uses the normal .venv.
test-local:
	UV_PROJECT_ENVIRONMENT=.venv-local uv run --only-group local pytest $(ARGS)

test-databricks:
	uv run --exact pytest $(ARGS)
