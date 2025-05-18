import pytest
from pathlib import Path
import json
from pyspark.sql import DataFrame
from pyspark.sql.connect.dataframe import DataFrame as DataFrameConn
from dsutils.dbc import configure_spark
from dsutils.gcp import spark_df_from_sheets


root_dir = Path(__file__).parent.parent.parent
clients = [f.name.split('.')[0] for f in root_dir.glob('config/*.json')]


@pytest.mark.parametrize('client', clients)
def test_control_sheet_exist(client):

    with open(f'config/{client}.json') as f:
        cfg = json.load(f)

    _ = configure_spark()
    df = spark_df_from_sheets(
        url=cfg['control_sheet']['url'],
        worksheet_name=cfg['control_sheet']['sheet'],
        gcp_scope=cfg['gcp']['scope'],
        gcp_key=cfg['gcp']['key'],
        schema=cfg['control_sheet']['read_schema']
    )

    assert isinstance(df, DataFrame | DataFrameConn)
