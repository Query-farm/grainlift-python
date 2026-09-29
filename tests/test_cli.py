# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""The development CLI: factory loading, token generation, and anonymous access."""

import subprocess
import sys
from importlib.metadata import distribution
from typing import Any

import pytest
from test_service import TestWorker

from grainlift import Worker
from grainlift.cli import load_worker, main, run


@pytest.fixture
def served(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Capture what the CLI would serve instead of binding a port."""
    captured: dict[str, Any] = {}

    def fake_serve(worker: Worker, **options: Any) -> None:
        captured.update(worker=worker, **options)

    def fake_granian(factory: str, **options: Any) -> None:
        captured.update(factory=factory, **options)

    monkeypatch.delenv("GRAINLIFT_TOKEN", raising=False)
    monkeypatch.setattr("grainlift.cli.serve", fake_serve)
    monkeypatch.setattr("grainlift.cli.serve_granian", fake_granian)
    return captured


def test_token_is_generated_and_printed(served: dict[str, Any], capsys: pytest.CaptureFixture[str]) -> None:
    """Token mode without GRAINLIFT_TOKEN generates one and prints the export line."""
    run("test_service:TestWorker", ["--port", "9001"])
    assert isinstance(served["worker"], TestWorker) and served["port"] == 9001
    assert served["anonymous_principal"] is None
    assert f"export GRAINLIFT_TOKEN={served['token']}" in capsys.readouterr().err


def test_environment_token_is_used(served: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    """An exported GRAINLIFT_TOKEN is served as-is."""
    monkeypatch.setenv("GRAINLIFT_TOKEN", "chosen")
    run("test_service:TestWorker", [])
    assert served["token"] == "chosen"


def test_anonymous_flag_serves_without_a_token(served: dict[str, Any], capsys: pytest.CaptureFixture[str]) -> None:
    """--auth anonymous needs no token and generates none."""
    run("test_service:TestWorker", ["--auth", "anonymous"])
    assert served["token"] is None and served["anonymous_principal"] == "anonymous"
    output = capsys.readouterr()
    assert "Anonymous access enabled" in output.out and "GRAINLIFT_TOKEN" not in output.err


def test_anonymous_default_still_accepts_an_exported_token(
    served: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A service defaulting to anonymous also accepts GRAINLIFT_TOKEN, and --auth token restores the requirement."""
    monkeypatch.setenv("GRAINLIFT_TOKEN", "chosen")
    run("test_service:TestWorker", [], auth="anonymous")
    assert served["token"] == "chosen" and served["anonymous_principal"] == "anonymous"
    run("test_service:TestWorker", ["--auth", "token"], auth="anonymous")
    assert served["token"] == "chosen" and served["anonymous_principal"] is None


def test_granian_anonymous(served: dict[str, Any]) -> None:
    """Granian receives anonymous access and no tokens."""
    main(["serve", "test_service:TestWorker", "--host", "granian", "--auth", "anonymous"])
    assert served["factory"] == "test_service:TestWorker"
    assert served["tokens"] is None and served["anonymous_principal"] == "anonymous"


def test_invalid_factories_and_options(served: dict[str, Any]) -> None:
    """Reject non-workers, malformed factories, incomplete mTLS options and unknown auth modes."""
    with pytest.raises(TypeError, match="Grainlift Worker"):
        load_worker("test_producer:ProducerConnection")
    with pytest.raises(ValueError, match="module:factory"):
        load_worker("test_service")
    with pytest.raises(SystemExit):
        run("test_service:TestWorker", ["--host", "mtls"])
    with pytest.raises(ValueError, match="auth must be"):
        run("test_service:TestWorker", [], auth="none")


def test_module_entry_point_and_no_console_script() -> None:
    """The CLI runs as ``python -m grainlift.cli``; the package claims no command name of its own."""
    completed = subprocess.run(
        [sys.executable, "-m", "grainlift.cli", "serve", "--help"],
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    )
    assert "usage: python -m grainlift.cli serve" in completed.stdout
    assert "--auth" in completed.stdout
    assert [entry for entry in distribution("grainlift").entry_points if entry.group == "console_scripts"] == []
