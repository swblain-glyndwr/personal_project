"""NextAds package.

The repo is moving toward a ``src/next_ads`` production package layout in
controlled steps. During the transition, the existing top-level package remains
the active import root, while future subpackages can be added under
``src/next_ads`` and imported as ``next_ads.<area>``.
"""

from next_ads._src_compat import extend_src_package_path

extend_src_package_path(__path__, __name__)
