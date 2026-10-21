import json
import random
from datetime import datetime, timedelta, timezone

import generate_telemetry as g


def _read(path):
    return [json.loads(line) for f in sorted(path.glob("*.json")) for line in f.read_text().splitlines()]


def test_fleet_is_deterministic():
    assert g.build_fleet(10, seed=1) == g.build_fleet(10, seed=1)
    assert len(g.build_fleet(25)) == 25


def test_batch_contains_duplicates_and_bad_rows():
    fleet = g.build_fleet(20)
    end = datetime.now(timezone.utc)
    recs, carry = g.generate_batch(fleet, end - timedelta(minutes=10), end, 2000,
                                   random.Random(7), dup_rate=0.05, bad_rate=0.1)
    ids = [r["event_id"] for r in recs if r["event_id"]]
    assert len(ids) > len(set(ids)), "expected duplicate event_ids"
    bad = [r for r in recs if r["device_id"] is None or r["temperature_c"] in (999.9, "ERR")
           or r["event_ts"] == "not-a-date"]
    assert bad, "expected corrupted records"
    assert carry, "expected carry-over sample for cross-file duplicates"


def test_registry_has_upsert_upgrade_and_delete():
    fleet = g.build_fleet(10)
    recs = g.registry_records(fleet, datetime.now(timezone.utc), 60)
    ops = [r["op"] for r in recs]
    assert ops.count("DELETE") == 1
    assert any(r["firmware_version"] == "1.1.0" for r in recs)


def test_main_writes_files_and_registry_only_once(tmp_path):
    args = ["--base-path", str(tmp_path), "--devices", "10", "--batches", "2", "--events-per-batch", "100"]
    g.main(args)
    g.main(args)
    assert len(list((tmp_path / "device_registry").glob("*.json"))) == 1
    assert len(list((tmp_path / "telemetry").glob("*.json"))) == 4
    assert _read(tmp_path / "telemetry")
