from catalyst_radar.textint.sentiment import score_sentiment


def test_positive_finance_phrase_scores_above_zero() -> None:
    assert score_sentiment("raises guidance and stronger demand") > 0


def test_negative_finance_phrase_scores_below_zero() -> None:
    assert score_sentiment("cuts guidance and regulatory investigation") < 0
