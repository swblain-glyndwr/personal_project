from next_ads.reporting import plotting


def test_reporting_plotting_exposes_directed_graph_plotter():
    assert plotting.DirectedGraphPlotter is not None
