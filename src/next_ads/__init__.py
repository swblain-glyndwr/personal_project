"""Future production package home for reusable NextAds code.

During the repo restructure, some modules still live in the legacy top-level
``next_ads`` package. If this ``src`` package is resolved first, keep the
legacy modules importable until their logic has moved.
"""

from pathlib import Path

_LEGACY_PACKAGE = Path(__file__).resolve().parents[2] / "next_ads"
if _LEGACY_PACKAGE.is_dir():
    _legacy_package_path = str(_LEGACY_PACKAGE)
    if _legacy_package_path not in __path__:
        __path__.append(_legacy_package_path)
