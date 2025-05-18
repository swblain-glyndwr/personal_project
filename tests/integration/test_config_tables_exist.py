import pytest
from pathlib import Path
import json
from dsutils.dbc import configure_spark
from dsutils.etl import map_tbl


root_dir = Path(__file__).parent.parent.parent
clients = [f.name.split('.')[0] for f in root_dir.glob('config/*.json')]


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
    dev_schema = cfg["schema"]["dev"]
    prod_schema = cfg["schema"]["prod"]

    for k, v in write_tables.items():
        v_mapped = map_tbl(v, dev_schema, client)
        msg = f'Read-write table does not exist in dev: {k}({v_mapped})'
        assert spark.catalog.tableExists(v_mapped), msg

    for k, v in write_tables.items():
        v_mapped = map_tbl(v, prod_schema, client)
        msg = f'Read-write table does not exist in prod: {k}({v})'
        assert spark.catalog.tableExists(v_mapped), msg
