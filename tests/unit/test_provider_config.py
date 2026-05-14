from catalyst_radar.core.config import AppConfig


def test_polygon_config_reads_env_without_requiring_key() -> None:
    config = AppConfig.from_env(
        {
            "CATALYST_MARKET_PROVIDER": "polygon",
            "CATALYST_POLYGON_API_KEY": "secret-key",
            "CATALYST_POLYGON_BASE_URL": "https://example.test",
            "CATALYST_HTTP_TIMEOUT_SECONDS": "7.5",
            "CATALYST_PROVIDER_AVAILABILITY_POLICY": "next_session_11_utc",
            "CATALYST_DAILY_PROVIDER": " polygon ",
            "CATALYST_DAILY_MARKET_PROVIDER": "sample",
            "CATALYST_CSV_SECURITIES_PATH": "fixtures/securities.csv",
            "CATALYST_CSV_DAILY_BARS_PATH": "fixtures/daily.csv",
            "CATALYST_CSV_HOLDINGS_PATH": "",
            "CATALYST_DAILY_EVENT_PROVIDER": "fixture",
            "CATALYST_NEWS_FIXTURE_PATH": "fixtures/news.json",
            "CATALYST_UNIVERSE_NAME": "liquid-us",
            "CATALYST_UNIVERSE_MIN_PRICE": "10",
            "CATALYST_UNIVERSE_MIN_AVG_DOLLAR_VOLUME": "25000000",
            "CATALYST_UNIVERSE_REQUIRE_SECTOR": "true",
        }
    )

    assert config.market_provider == "polygon"
    assert config.polygon_api_key == "secret-key"
    assert config.polygon_base_url == "https://example.test"
    assert config.http_timeout_seconds == 7.5
    assert config.provider_availability_policy == "next_session_11_utc"
    assert config.daily_provider == "polygon"
    assert config.daily_market_provider == "sample"
    assert config.csv_securities_path == "fixtures/securities.csv"
    assert config.csv_daily_bars_path == "fixtures/daily.csv"
    assert config.csv_holdings_path is None
    assert config.daily_event_provider == "fixture"
    assert config.news_fixture_path == "fixtures/news.json"
    assert config.universe_name == "liquid-us"
    assert config.universe_min_price == 10
    assert config.universe_min_avg_dollar_volume == 25_000_000
    assert config.universe_require_sector is True


def test_invalid_provider_timeout_fails_fast() -> None:
    try:
        AppConfig.from_env({"CATALYST_HTTP_TIMEOUT_SECONDS": "0"})
    except ValueError as exc:
        assert "CATALYST_HTTP_TIMEOUT_SECONDS" in str(exc)
    else:
        raise AssertionError("expected invalid timeout to fail")
