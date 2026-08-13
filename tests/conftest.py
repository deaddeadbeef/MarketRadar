"""Default test env keeps the legacy workbench CLI runnable.

Product tests that check the hard-block must unset CATALYST_ENABLE_LEGACY_WORKBENCH.
"""

from __future__ import annotations

import os

os.environ.setdefault("CATALYST_ENABLE_LEGACY_WORKBENCH", "true")
