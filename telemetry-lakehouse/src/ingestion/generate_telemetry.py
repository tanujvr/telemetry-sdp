"""IoT telemetry simulator.

Writes newline-delimited JSON files into a landing zone (a Unity Catalog Volume
path on Databricks, or any local folder for dry-runs):

    <base-path>/telemetry/*.json        raw sensor readings (append-only)
    <base-path>/device_registry/*.json  device master data as CDC events (UPSERT / DELETE)

It deliberately injects real-world mess so the pipeline has something to clean:
duplicates (same-file and cross-file), malformed values, missing keys, bad and
future timestamps, plus genuine anomalies (temperature / vibration spikes,
low battery).

Run locally:
    python src/ingestion/generate_telemetry.py --base-path ./landing --batches 2
"""
from __future__ import annotations

import argparse
import json
import random
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

SITES = ["Mumbai-Plant", "Pune-Plant", "Chennai-Plant", "Delhi-Hub"]
# device_type -> (baseline temperature C, baseline vibration mm/s)
PROFILES = {
    "compressor": (62.0, 2.5),
    "pump": (48.0, 2.0),
    "motor": (70.0, 3.0),
    "chiller": (8.0, 1.0),
}
CORRUPTIONS = [
    "missing_device_id",
    "null_event_id",
    "temp_out_of_range",
    "temp_not_numeric",
    "bad_timestamp",
    "future_timestamp",
]


@dataclass(frozen=True)
class Device:
    device_id: str
    site: str
    device_type: str
    firmware_version: str
    base_temp: float
    base_vib: float
    battery_offset: float
    drain_per_hour: float


def iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def build_fleet(n: int, seed: int = 42) -> list[Device]:
    rng = random.Random(seed)
    fleet = []
    for i in range(1, n + 1):
        dtype = rng.choice(list(PROFILES))
        temp, vib = PROFILES[dtype]
        fleet.append(
            Device(
                device_id=f"dev-{i:04d}",
                site=rng.choice(SITES),
                device_type=dtype,
                firmware_version="1.0.0",
                base_temp=temp + rng.uniform(-3, 3),
                base_vib=vib + rng.uniform(-0.3, 0.3),
                battery_offset=rng.uniform(0, 90),
                drain_per_hour=rng.uniform(0.2, 1.5),
            )
        )
    return fleet


def registry_records(fleet: list[Device], now: datetime, window_minutes: int, seed: int = 42) -> list[dict]:
    """Initial UPSERT per device, ~20% firmware upgrades mid-window, one decommission."""
    rng = random.Random(seed + 1)
    records = [
        {
            "device_id": d.device_id,
            "site": d.site,
            "device_type": d.device_type,
            "firmware_version": d.firmware_version,
            "op": "UPSERT",
            "updated_at": iso(now - timedelta(days=30)),
        }
        for d in fleet
    ]
    for d in rng.sample(fleet, k=max(1, len(fleet) // 5)):
        records.append(
            {
                "device_id": d.device_id,
                "site": d.site,
                "device_type": d.device_type,
                "firmware_version": "1.1.0",
                "op": "UPSERT",
                "updated_at": iso(now - timedelta(minutes=window_minutes / 2)),
            }
        )
    if len(fleet) > 1:
        records.append(
            {
                "device_id": fleet[-1].device_id,
                "site": None,
                "device_type": None,
                "firmware_version": None,
                "op": "DELETE",
                "updated_at": iso(now - timedelta(minutes=1)),
            }
        )
    return records


def make_event(dev: Device, ts: datetime, rng: random.Random) -> dict:
    spike = rng.random() < 0.02
    temp = rng.gauss(dev.base_temp, 1.5) + (rng.uniform(25, 45) if spike else 0.0)
    vib = abs(rng.gauss(dev.base_vib, 0.4)) + (rng.uniform(4, 8) if spike else 0.0)
    hours = ts.timestamp() / 3600.0
    battery = 100.0 - ((hours + dev.battery_offset) * dev.drain_per_hour) % 90.0
    return {
        "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "device_id": dev.device_id,
        "event_ts": iso(ts),
        "temperature_c": round(temp, 2),
        "humidity_pct": round(min(100.0, max(0.0, rng.gauss(45, 8))), 1),
        "vibration_mm_s": round(vib, 3),
        "battery_pct": round(battery, 1),
        "signal_dbm": round(rng.gauss(-65, 12), 1),
        "firmware_version": dev.firmware_version,
        "status": "RUNNING" if rng.random() > 0.03 else "IDLE",
    }


def corrupt(event: dict, now: datetime, rng: random.Random) -> dict:
    bad = dict(event)
    mode = rng.choice(CORRUPTIONS)
    if mode == "missing_device_id":
        bad["device_id"] = None
    elif mode == "null_event_id":
        bad["event_id"] = None
    elif mode == "temp_out_of_range":
        bad["temperature_c"] = 999.9
    elif mode == "temp_not_numeric":
        bad["temperature_c"] = "ERR"
    elif mode == "bad_timestamp":
        bad["event_ts"] = "not-a-date"
    elif mode == "future_timestamp":
        bad["event_ts"] = iso(now + timedelta(days=1))
    return bad


def generate_batch(
    fleet: list[Device],
    start: datetime,
    end: datetime,
    n_events: int,
    rng: random.Random,
    dup_rate: float = 0.02,
    bad_rate: float = 0.03,
    carry_over: list[dict] | None = None,
    now: datetime | None = None,
) -> tuple[list[dict], list[dict]]:
    """Return (records_to_write, sample_for_next_batch_duplicates)."""
    now = now or end
    span = (end - start).total_seconds()
    events = [
        make_event(rng.choice(fleet), start + timedelta(seconds=rng.uniform(0, span)), rng)
        for _ in range(n_events)
    ]
    records = [corrupt(e, now, rng) if rng.random() < bad_rate else e for e in events]
    records += [dict(e) for e in rng.sample(events, k=int(len(events) * dup_rate))]  # same-file dups
    records += carry_over or []  # cross-file dups
    rng.shuffle(records)
    next_carry = [dict(e) for e in rng.sample(events, k=int(len(events) * dup_rate))]
    return records, next_carry


def write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


def main(argv: list[str] | None = None) -> dict:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--base-path", default="./landing", help="Landing root (UC Volume path or local folder)")
    p.add_argument("--devices", type=int, default=50)
    p.add_argument("--batches", type=int, default=3, help="Number of telemetry files to write")
    p.add_argument("--events-per-batch", type=int, default=2000)
    p.add_argument("--window-minutes", type=int, default=60, help="Event-time window covered by this run (ending now)")
    p.add_argument("--dup-rate", type=float, default=0.02)
    p.add_argument("--bad-rate", type=float, default=0.03)
    p.add_argument("--seed", type=int, default=42, help="Seed for the (stable) device fleet")
    p.add_argument("--force-registry", action="store_true", help="Rewrite device registry even if one exists")
    p.add_argument("--sleep-seconds", type=float, default=0.0, help="Pause between files (simulate streaming)")
    args, _ = p.parse_known_args(argv)

    base = Path(args.base_path)
    now = datetime.now(timezone.utc)
    fleet = build_fleet(args.devices, args.seed)

    # device registry: first run only (it is CDC data, re-sending would just be noise)
    reg_dir = base / "device_registry"
    existing = list(reg_dir.glob("*.json")) if reg_dir.exists() else []
    registry_written = False
    if not existing or args.force_registry:
        write_jsonl(
            reg_dir / f"registry_{now:%Y%m%dT%H%M%S}.json",
            registry_records(fleet, now, args.window_minutes, args.seed),
        )
        registry_written = True

    rng = random.Random()  # events differ every run
    start = now - timedelta(minutes=args.window_minutes)
    step = timedelta(minutes=args.window_minutes / args.batches)
    carry: list[dict] = []
    total = 0
    for i in range(args.batches):
        b_start, b_end = start + i * step, start + (i + 1) * step
        records, carry = generate_batch(
            fleet, b_start, b_end, args.events_per_batch, rng,
            args.dup_rate, args.bad_rate, carry, now=now,
        )
        fname = f"telemetry_{now:%Y%m%dT%H%M%S}_{i:03d}_{uuid.uuid4().hex[:6]}.json"
        write_jsonl(base / "telemetry" / fname, records)
        total += len(records)
        if args.sleep_seconds and i < args.batches - 1:
            time.sleep(args.sleep_seconds)

    summary = {
        "base_path": str(base),
        "telemetry_files": args.batches,
        "telemetry_records": total,
        "registry_written": registry_written,
    }
    print(json.dumps(summary))
    return summary


if __name__ == "__main__":
    main()
