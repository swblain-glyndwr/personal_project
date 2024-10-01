from src.Config import Config
from src.ControlSheet import ControlSheet
from utils.dbcutils import get_display


# Config
cfg = Config("dev")

# Process control file
ctrl = ControlSheet(
    url=cfg.resources["control"]["url"],
    worksheet_name=cfg.resources["control"]["sheet"],
    schema=cfg.resources["control"]["read_schema"]
    )

df = ctrl.get_df()
# ctrl.process_control_sheet
# ctrl.write_to_history()
# ctrl.write_to_latest()
get_display(ctrl.df)
# Overall Control and Division


# Iterative Page Build


# Results
