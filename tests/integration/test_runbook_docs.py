from pathlib import Path

RUNBOOKS_WITH_LOCAL_API_COMMANDS = [
    Path("docs/runbooks/radar-run.md"),
    Path("docs/runbooks/schwab.md"),
    Path("docs/runbooks/score-drift.md"),
    Path("docs/runbooks/provider-failure.md"),
    Path("docs/runbooks/llm-failure.md"),
]


def test_radar_run_runbook_uses_windows_compatible_local_api_commands() -> None:
    text = Path("docs/runbooks/radar-run.md").read_text(encoding="utf-8")

    assert "curl.exe --insecure --fail" in text
    assert "Run Fixture Smoke" in text
    assert "Run Capped Live Radar" in text
    assert "Invoke-RestMethod" not in text
    assert "SkipCertificateCheck" not in text


def test_local_api_runbook_commands_use_current_https_endpoint() -> None:
    for path in RUNBOOKS_WITH_LOCAL_API_COMMANDS:
        text = path.read_text(encoding="utf-8")

        assert "curl.exe --insecure --fail" in text
        assert "Invoke-RestMethod" not in text
        assert "SkipCertificateCheck" not in text
        assert "http://localhost:8000" not in text
        assert "http://127.0.0.1:8000" not in text
