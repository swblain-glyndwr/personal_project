import importlib

from next_ads.reporting import plotting as new_plotting


def test_reporting_plotting_imports_work_from_new_and_legacy_paths():
    legacy_plotting = importlib.import_module("next_ads.Plotting")

    assert (
        legacy_plotting.DirectedGraphPlotter
        is new_plotting.DirectedGraphPlotter
    )
