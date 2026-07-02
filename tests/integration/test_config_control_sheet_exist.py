import pytest
from pyspark.sql import DataFrame
from pyspark.sql.connect.dataframe import DataFrame as DataFrameConn
from dsutils.dbc import configure_spark
from dsutils.gcp import spark_df_from_sheets
from next_ads.common.paths import iter_client_config_paths, load_client_config


clients = [path.stem for path in iter_client_config_paths()]


@pytest.mark.parametrize('client', clients)
def test_control_sheet_exist(client):

    cfg = load_client_config(client)

    _ = configure_spark()
    df = spark_df_from_sheets(
        url=cfg['control_sheet']['url'],
        worksheet_name=cfg['control_sheet']['sheet'],
        gcp_scope=cfg['gcp']['scope'],
        gcp_key=cfg['gcp']['key'],
        schema=cfg['control_sheet']['read_schema']
    )

    assert isinstance(df, DataFrame | DataFrameConn)
