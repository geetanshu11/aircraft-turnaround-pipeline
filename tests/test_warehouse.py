import pytest
from turnaround.warehouse import connect, load_partition, health, initialize
from turnaround.core import Config
from .test_ingestion import prepare
from .helpers import row, second

pytestmark = pytest.mark.postgres


def test_revision_rollback_reactivation_watermark(pg, tmp_path):
    a = prepare(pg, tmp_path, [row(), second()], name="a.zip")
    assert load_partition(pg, a, "first")["inserted"] == 2
    assert load_partition(pg, a, "rerun")["status"] == "unchanged"
    b = prepare(pg, tmp_path, [row(Tail_Number="N999AA")], name="b.zip")
    with pytest.raises(RuntimeError, match="Injected"):
        load_partition(pg, b, "failed", fail_before_commit=True)
    with connect(pg) as c:
        assert c.execute("SELECT count(*) AS n FROM raw.flights").fetchone()["n"] == 2
        assert c.execute("SELECT count(*) AS n FROM meta.flight_changes").fetchone()["n"] == 0
        assert c.execute("SELECT version_id FROM meta.checkpoints").fetchone()["version_id"] == a["version_id"]
    r = load_partition(pg, b, "retry")
    assert r["updated"] == 1 and r["withdrawn"] == 1
    r = load_partition(pg, a, "reactivation")
    assert r["updated"] == 1 and r["inserted"] == 1
    with connect(pg) as c:
        assert c.execute("SELECT count(*) n FROM meta.partition_versions").fetchone()["n"] == 2
        assert c.execute("SELECT count(*) n FROM meta.partition_activations").fetchone()["n"] == 3
        assert c.execute("SELECT count(*) n FROM meta.flight_changes").fetchone()["n"] == 3
    assert health(pg)["maximum_loaded_month"] == "2025-03"


def test_database_mode_guard(pg):
    with pytest.raises(ValueError, match="mix"):
        initialize(Config(pg.root.parent / "demo", mode="demo"))
