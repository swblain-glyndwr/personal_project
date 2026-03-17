import pytest
from pathlib import Path
import json
from dsutils.dbc import configure_spark
from next_ads.utils import etl


root_dir = Path(__file__).parent.parent.parent
clients = [f.name.split('.')[0] for f in root_dir.glob('config/next_uk.json')]


@pytest.mark.parametrize('client', clients)
def test_read_tables_exist(client):

    spark = configure_spark()
    with open(f"config/{client}.json") as f:
        cfg = json.load(f)

    read_tables = cfg["tables"]["read"]

    for k, v in read_tables.items():
        msg = f'Read-only table does not exist: {k}({v})'
        assert spark.catalog.tableExists(v), msg


@pytest.mark.parametrize('client', clients)
def test_write_tables_exist(client):

    spark = configure_spark()
    with open(f"config/{client}.json") as f:
        cfg = json.load(f)

    write_tables = cfg["tables"]["write"]
    prod_schema = cfg["schema"]["prod"]
    prod_catalog = "marketingdata_prod"

    for k, v in write_tables.items():
        v_mapped = etl.map_tbl(v, schema=prod_schema, client=client, catalog=prod_catalog)
        msg = f'Read-write table does not exist in prod: {k}({v_mapped})'
        assert spark.catalog.tableExists(v_mapped), msg
