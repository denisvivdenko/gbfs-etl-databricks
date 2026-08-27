"""Smoke test: trigger the deployed dev pipeline and confirm it completes.

Run with: uv run pytest tests/integration -m integration --db-profile <profile>

Requires `databricks bundle deploy -t dev --profile <profile>` to have been
run first, so `databricks bundle summary` has a deployment to read from.
"""

import time

import pytest

pytestmark = pytest.mark.integration

TERMINAL_STATES = {"COMPLETED", "FAILED", "CANCELED"}
POLL_INTERVAL_SECONDS = 15
TIMEOUT_SECONDS = 15 * 60


def test_pipeline_completes_successfully(workspace_client, pipeline_id: str):
    update = workspace_client.pipelines.start_update(pipeline_id=pipeline_id)

    deadline = time.monotonic() + TIMEOUT_SECONDS
    state = None
    while time.monotonic() < deadline:
        status = workspace_client.pipelines.get_update(pipeline_id=pipeline_id, update_id=update.update_id)
        state = status.update.state.value
        if state in TERMINAL_STATES:
            break
        time.sleep(POLL_INTERVAL_SECONDS)
    else:
        pytest.fail(
            f"Pipeline update {update.update_id} did not reach a terminal state "
            f"within {TIMEOUT_SECONDS}s (last state={state})"
        )

    if state == "FAILED":
        events = workspace_client.pipelines.list_pipeline_events(pipeline_id=pipeline_id)
        errors = [e for e in events if e.level and e.level.value == "ERROR"][:5]
        details = "\n".join(
            f"- {e.message}: "
            f"{(e.error.exceptions[0].message if e.error and e.error.exceptions else 'no exception body')[:500]}"
            for e in errors
        )
        pytest.fail(f"Pipeline update {update.update_id} FAILED:\n{details}")
    assert state == "COMPLETED"
