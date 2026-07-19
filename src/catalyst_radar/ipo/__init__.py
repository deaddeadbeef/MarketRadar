"""DEPRECATED product surface: IPO/S-1 analysis.

Out of event-first discovery scope. See docs/PRODUCT_SCOPE.md and docs/DEPRECATION.md.
"""

from catalyst_radar.ipo.s1 import (
    analyze_s1_offering,
    is_ipo_registration_form,
    strip_sec_html,
    summarize_s1_analysis,
)

__all__ = [
    "analyze_s1_offering",
    "is_ipo_registration_form",
    "strip_sec_html",
    "summarize_s1_analysis",
]
