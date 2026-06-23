"""Compatibility package for data code moving into ``src/next_ads/data``."""

from next_ads._src_compat import extend_src_package_path

extend_src_package_path(__path__, __name__)
