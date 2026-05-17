from datetime import UTC, datetime

from catalyst_radar.core.models import MarketFeatures
from catalyst_radar.scoring.priced_in import evaluate_priced_in


def test_priced_in_flags_bullish_emotion_ahead_of_price_reaction() -> None:
    result = evaluate_priced_in(
        _features(ret_5d=0.01, ret_20d=0.02, rs_20_sector=52.0, rs_60_spy=53.0),
        _high_bullish_emotion(),
    )

    assert result.status == "bullish_not_priced_in"
    assert result.direction == "bullish"
    assert result.emotion_score > result.reaction_score
    assert result.priced_in_score < 50.0
    assert "ahead of price reaction" in result.reason


def test_priced_in_flags_fully_priced_when_reaction_matches_emotion() -> None:
    result = evaluate_priced_in(
        _features(
            ret_5d=0.10,
            ret_20d=0.20,
            rs_20_sector=80.0,
            rs_60_spy=78.0,
            rel_volume_5d=2.0,
            dollar_volume_z=2.0,
            extension_20d=0.08,
        ),
        _high_bullish_emotion(),
    )

    assert result.status == "fully_priced"
    assert result.priced_in_score == 100.0
    assert result.reaction_score >= result.emotion_score - 10.0


def test_priced_in_flags_overextended_hype_when_price_moves_without_emotion() -> None:
    result = evaluate_priced_in(
        _features(
            ret_5d=0.14,
            ret_20d=0.28,
            rs_20_sector=88.0,
            rs_60_spy=84.0,
            rel_volume_5d=2.5,
            dollar_volume_z=2.5,
            extension_20d=0.14,
        ),
        {
            "event_support_score": 5.0,
            "material_event_count": 0,
            "local_narrative_score": 5.0,
            "sentiment_score": 4.0,
            "novelty_score": 0.0,
            "source_quality_score": 10.0,
            "theme_match_score": 0.0,
            "options_flow_score": 0.0,
            "theme_velocity_score": 0.0,
        },
    )

    assert result.status == "overextended_hype"
    assert result.reaction_score > result.emotion_score
    assert "chase risk" in result.reason


def test_priced_in_handles_bearish_emotion_not_priced_in() -> None:
    result = evaluate_priced_in(
        _features(ret_5d=-0.01, ret_20d=-0.02, rs_20_sector=48.0, rs_60_spy=47.0),
        {**_high_bullish_emotion(), "sentiment_score": -62.0},
    )

    assert result.status == "bearish_not_priced_in"
    assert result.direction == "bearish"
    assert result.emotion_reaction_gap > 0.0


def test_priced_in_fails_closed_on_stale_data() -> None:
    result = evaluate_priced_in(
        _features(ret_5d=0.01, ret_20d=0.02),
        _high_bullish_emotion(),
        data_stale=True,
    )

    assert result.status == "stale"
    assert result.evidence["data_stale"] is True
    assert result.next_step == "Refresh latest market bars, then re-run the radar."


def _high_bullish_emotion() -> dict[str, object]:
    return {
        "event_support_score": 90.0,
        "material_event_count": 1,
        "local_narrative_score": 80.0,
        "sentiment_score": 60.0,
        "novelty_score": 75.0,
        "source_quality_score": 100.0,
        "theme_match_score": 70.0,
        "options_flow_score": 10.0,
        "theme_velocity_score": 60.0,
    }


def _features(
    *,
    ret_5d: float = 0.0,
    ret_20d: float = 0.0,
    rs_20_sector: float = 50.0,
    rs_60_spy: float = 50.0,
    rel_volume_5d: float = 1.0,
    dollar_volume_z: float = 0.0,
    extension_20d: float = 0.0,
) -> MarketFeatures:
    return MarketFeatures(
        ticker="AAA",
        as_of=datetime(2026, 5, 8, 21, tzinfo=UTC),
        ret_5d=ret_5d,
        ret_20d=ret_20d,
        rs_20_sector=rs_20_sector,
        rs_60_spy=rs_60_spy,
        near_52w_high=0.80,
        ma_regime=50.0,
        rel_volume_5d=rel_volume_5d,
        dollar_volume_z=dollar_volume_z,
        atr_pct=0.04,
        extension_20d=extension_20d,
        liquidity_score=75.0,
        feature_version="market-v1",
    )
