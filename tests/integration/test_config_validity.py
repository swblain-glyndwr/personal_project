import pytest
from pathlib import Path
import json


root_dir = Path(__file__).parent.parent.parent
clients = [f.name.split('.')[0] for f in root_dir.glob('config/*.json')]


@pytest.mark.parametrize('client', clients)
def test_audiences_required(client):

    with open(f'config/{client}.json') as f:
        cfg = json.load(f)

    if 'Audiences' in cfg['transient_cells']:
        audiences_required = [
            x[0] for x in cfg['transient_cells']['Audiences']]
        read_tables = list(cfg['tables']['read'].keys())

        for audience in audiences_required:
            msg = f'Audience key not in read tables: {audience}'
            assert audience in read_tables, msg
