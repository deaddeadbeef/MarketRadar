"""DEPRECATED as primary product surface: alert digests and routing.

Optional later enrichment; not discovery core. See docs/PRODUCT_SCOPE.md.
"""

from catalyst_radar.alerts.models import (
    Alert,
    AlertChannel,
    AlertPriority,
    AlertRoute,
    AlertStatus,
    AlertSuppression,
    UserFeedback,
    alert_id,
    alert_suppression_id,
    user_feedback_id,
)

__all__ = [
    "Alert",
    "AlertChannel",
    "AlertPriority",
    "AlertRoute",
    "AlertStatus",
    "AlertSuppression",
    "UserFeedback",
    "alert_id",
    "alert_suppression_id",
    "user_feedback_id",
]
