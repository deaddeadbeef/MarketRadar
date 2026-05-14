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
    assert "curl.exe" in text
    assert "--insecure" in text
    assert "--fail" in text
    assert "ServerCertificateValidationCallback" not in text
    assert "SkipCertificateCheck" not in text


def test_readme_mentions_restart_script_for_local_dashboard() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")

    assert "scripts/restart-local.ps1" in readme
    assert "scripts/check-live-activation.ps1" in readme


def test_check_live_activation_script_is_zero_external_call_status_check() -> None:
    script = Path("scripts/check-live-activation.ps1")
    text = script.read_text(encoding="utf-8")

    assert script.is_file()
    assert "/api/radar/live-activation" in text
    assert "curl.exe" in text
    assert "--insecure" in text
    assert "--fail" in text
    assert "External calls made by this check: 0" in text
    assert "missing_env" in text
    assert "operator_steps" in text
    assert "OPENAI_API_KEY=" not in text
    assert "CATALYST_POLYGON_API_KEY=" not in text
    assert "SCHWAB_CLIENT_SECRET=" not in text
