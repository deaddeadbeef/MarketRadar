from __future__ import annotations

from catalyst_radar.ipo.s1 import (
    analyze_s1_offering,
    is_ipo_registration_form,
    strip_sec_html,
    summarize_s1_analysis,
)


def test_is_ipo_registration_form_detects_s1_variants() -> None:
    assert is_ipo_registration_form("S-1") is True
    assert is_ipo_registration_form("s-1/a") is True
    assert is_ipo_registration_form("10-Q") is False


def test_strip_sec_html_removes_markup_and_scripts() -> None:
    text = strip_sec_html(
        "<html><script>ignored()</script><style>bad</style><p>Acme&nbsp;Robotics</p></html>"
    )

    assert text == "Acme Robotics"


def test_analyze_s1_offering_extracts_core_terms_and_risks() -> None:
    analysis = analyze_s1_offering(
        """
        <html><body>
        <h1>Prospectus Summary</h1>
        Acme Robotics, Inc. We are offering 12,500,000 shares of Class A common stock.
        The initial public offering price is expected to be between $17.00 and $19.00 per share.
        We have applied to list our Class A common stock on the Nasdaq Global Select Market
        under the symbol "ACME".
        Morgan Stanley & Co. LLC and Goldman Sachs & Co. LLC are acting as lead
        book-running managers.
        <h1>Use of Proceeds</h1>
        We intend to use the net proceeds from this offering for working capital,
        research and development, sales and marketing and general corporate purposes.
        <h1>Risk Factors</h1>
        We have a history of losses and may never achieve profitability.
        Our revenue is concentrated among a limited number of customers.
        We are an emerging growth company.
        Holders of Class B common stock will maintain voting control after this offering.
        </body></html>
        """,
        company_name="Acme Robotics, Inc.",
        ticker=None,
        form_type="S-1",
        source_url="https://www.sec.gov/Archives/acme.htm",
    )

    assert analysis["analysis_version"] == "ipo-s1-analysis-v1"
    assert analysis["company_name"] == "Acme Robotics, Inc."
    assert analysis["proposed_ticker"] == "ACME"
    assert analysis["exchange"] == "Nasdaq Global Select Market"
    assert analysis["shares_offered"] == 12_500_000
    assert analysis["price_range_low"] == 17.0
    assert analysis["price_range_high"] == 19.0
    assert analysis["price_range_midpoint"] == 18.0
    assert analysis["estimated_gross_proceeds"] == 225_000_000.0
    assert analysis["underwriters"] == [
        "Morgan Stanley & Co. LLC",
        "Goldman Sachs & Co. LLC",
    ]
    assert analysis["risk_flags"] == [
        "history_of_losses",
        "customer_concentration",
        "dual_class_or_controlled_company",
        "emerging_growth_company",
    ]
    assert analysis["sections_found"] == [
        "prospectus summary",
        "risk factors",
        "use of proceeds",
    ]
    assert "working capital" in str(analysis["use_of_proceeds_summary"])
    assert "12,500,000 shares offered" in summarize_s1_analysis(analysis)


def test_analyze_s1_offering_missing_fields_returns_empty_values() -> None:
    analysis = analyze_s1_offering(
        "Registration statement with limited details.",
        company_name=None,
        ticker=None,
        form_type="S-1/A",
        source_url=None,
    )

    assert analysis["form_type"] == "S-1/A"
    assert analysis["proposed_ticker"] is None
    assert analysis["shares_offered"] is None
    assert analysis["price_range_low"] is None
    assert analysis["underwriters"] == []
    assert analysis["risk_flags"] == []
