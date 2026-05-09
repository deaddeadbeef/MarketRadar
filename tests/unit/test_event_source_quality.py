from catalyst_radar.events.models import SourceCategory
from catalyst_radar.events.source_quality import score_source_quality


def test_primary_sources_score_highest() -> None:
    result = score_source_quality(
        source="SEC EDGAR",
        category=SourceCategory.PRIMARY_SOURCE,
        url="https://www.sec.gov/Archives/example",
    )

    assert result.score == 1.0
    assert "primary_source" in result.reasons


def test_promotional_source_scores_low() -> None:
    result = score_source_quality(
        source="Sponsored Stocks Daily",
        category=SourceCategory.PROMOTIONAL,
        url="https://promo.example.com/msft",
    )

    assert result.score <= 0.2
    assert "promotional_source" in result.reasons
