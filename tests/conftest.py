import os
import pytest
from turnaround.core import Config, reference_snapshot


@pytest.fixture
def cfg(tmp_path):
    c = Config(tmp_path / "runtime", mode="fixture", chunk_rows=2)
    c.initialize()
    return c


@pytest.fixture
def airports(cfg):
    return reference_snapshot(cfg)[0]


@pytest.fixture
def pg(cfg):
    if os.getenv("TEST_POSTGRES") != "1":
        pytest.skip("Set TEST_POSTGRES=1 with a dedicated disposable database")
    from turnaround.warehouse import connect, initialize

    with connect(cfg, autocommit=True) as con:
        con.execute(
            "DROP SCHEMA IF EXISTS analytics CASCADE; DROP SCHEMA IF EXISTS raw CASCADE; DROP SCHEMA IF EXISTS meta CASCADE"
        )
    initialize(cfg)
    return cfg
