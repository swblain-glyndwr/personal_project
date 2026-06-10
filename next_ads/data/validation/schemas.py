"""Compatibility wrapper for data validation schemas."""

from next_ads.data.validation._src_loader import load_src_validation_module

_schemas = load_src_validation_module("schemas")

ControlSheetExclusionsInputModel = _schemas.ControlSheetExclusionsInputModel
ControlSheetInputModel = _schemas.ControlSheetInputModel
ControlSheetInputModelv2 = _schemas.ControlSheetInputModelv2
ControlSheetPLXInputModel = _schemas.ControlSheetPLXInputModel
ControlSheetPlacementsInputModel = _schemas.ControlSheetPlacementsInputModel
GlobalSolutionOutputModel = _schemas.GlobalSolutionOutputModel

__all__ = [
    "ControlSheetExclusionsInputModel",
    "ControlSheetInputModel",
    "ControlSheetInputModelv2",
    "ControlSheetPLXInputModel",
    "ControlSheetPlacementsInputModel",
    "GlobalSolutionOutputModel",
]
