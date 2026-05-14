from pathlib import Path


def test_restart_local_script_restarts_only_market_radar_processes() -> None:
    script = Path("scripts/restart-local.ps1")
    text = script.read_text(encoding="utf-8")

    assert script.is_file()
    assert "apps.api.main:app" in text
    assert "apps/dashboard/Home.py" in text
    assert "apps\\.api\\.main|apps/dashboard/Home\\.py" in text
    assert ".state\\processes" in text
    assert "data\\local\\schwab-localhost-key.pem" in text
    assert "data\\local\\schwab-localhost-cert.pem" in text
    assert "PYTHONPATH" in text
    assert "-Environment" not in text
    assert "Invoke-WebRequest" in text
    assert "ServerCertificateValidationCallback" in text
    assert "SkipCertificateCheck" not in text


def test_readme_mentions_restart_script_for_local_dashboard() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")

    assert "scripts/restart-local.ps1" in readme
