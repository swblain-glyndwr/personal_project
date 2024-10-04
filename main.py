from src.Config import Config
from src.ControlSheet import ControlSheet
from utils.dbcutils import get_display
from src.NextAdsPage import NextAdsPage

# Config
cfg = Config("dev")

# Process control file
ctrl = ControlSheet(
    url=cfg.resources["control_sheet"]["url"],
    worksheet_name=cfg.resources["control_sheet"]["sheet"],
    schema=cfg.resources["control_sheet"]["read_schema"]
    )

ctrl.get_sheet()
# ctrl.process_sheet()
# ctrl.write_to_tables()

get_display(ctrl.df)

# Overall Control and Division


# Iterative Page Build
div_dict = {
    "womens": "personalised_random",
    "mens": "personalised_random",
    "girls": "personalised_random",
    "boys": "personalised_random",

}
homepage = NextAdsPage(
    "HN1",
    div_dict)

# Results
