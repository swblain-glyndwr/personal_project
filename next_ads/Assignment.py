"""Compatibility wrapper for decisioning assignment helpers."""

import sys

from next_ads.decisioning import assignment as _assignment

sys.modules[__name__] = _assignment
