from pathlib import Path


def test_radar_run_runbook_uses_windows_compatible_local_api_commands() -> None:
    text = Path("docs/runbooks/radar-run.md").read_text(encoding="utf-8")

    assert "curl.exe --insecure --fail" in text
    assert "Run Fixture Smoke" in text
    assert "Run Capped Live Radar" in text
    assert "Invoke-RestMethod" not in text
    assert "SkipCertificateCheck" not in text
