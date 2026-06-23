"""Compatibility imports for the moved data validation package.

This path is needed while Ads v2 work still carries a legacy
``next_ads.data`` package into the merge.
"""

from . import custom_checks, schemas

__all__ = ["custom_checks", "schemas"]
