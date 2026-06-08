"""NextAds package.

The repo is moving toward a ``src/next_ads`` production package layout in
controlled steps. During the transition, the existing top-level package remains
the active import root, while future subpackages can be added under
``src/next_ads`` and imported as ``next_ads.<area>``.
"""

from pathlib import Path

_SRC_PACKAGE = Path(__file__).resolve().parents[1] / "src" / "next_ads"
if _SRC_PACKAGE.is_dir():
    _src_package_path = str(_SRC_PACKAGE)
    if _src_package_path not in __path__:
        __path__.append(_src_package_path)
