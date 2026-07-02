import pytest
from next_ads.common.paths import iter_client_config_paths, load_client_config


clients = [path.stem for path in iter_client_config_paths()]


@pytest.mark.parametrize('client', clients)
def test_audiences_required(client):

    cfg = load_client_config(client)

    if 'Audiences' in cfg['transient_cells']:
        audiences_required = [
            x[0] for x in cfg['transient_cells']['Audiences']]
        read_tables = list(cfg['tables']['read'].keys())

        for audience in audiences_required:
            msg = f'Audience key not in read tables: {audience}'
            assert audience in read_tables, msg
