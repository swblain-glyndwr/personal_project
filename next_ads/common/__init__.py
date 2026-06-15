"""Compatibility package for shared code moving into ``src/next_ads/common``."""

from next_ads._src_compat import extend_src_package_path

extend_src_package_path(__path__, __name__)
