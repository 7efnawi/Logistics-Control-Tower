"""
Logistics Control Tower - Simulator v1

Modes:
  sample    small dataset (default 20 shipments) written at once  -> send to the team on day 1
  backfill  3 months of history written at once                   -> for carrier scorecards
  stream    drops small JSON files into a folder every few seconds -> Spark reads it as a stream

Output (JSON Lines, one record per line, files end with .json):
  <out>/batch/carriers.json
  <out>/batch/ports.json
  <out>/batch/shipments.json
  <out>/stream/events/events_000001.json   status events
  <out>/stream/gps/gps_000001.json         GPS pings

Examples:
  python generate.py --mode sample
  python generate.py --mode backfill --shipments 3000 --days 90
  python generate.py --mode stream --shipments 100 --interval 5 --speed 600
"""
import argparse
import json
import os
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

STATUSES = ["picked_up", "at_port", "customs", "out_for_delivery", "delivered"]

# ---- master data -----------------------------------------------------------
PORTS = [
    {"port_id": "ALX", "name": "Alexandria", "country": "EG", "lat": 31.20, "lon": 29.92},
    {"port_id": "DAM", "name": "Damietta", "country": "EG", "lat": 31.42, "lon": 31.81},
    {"port_id": "PSD", "name": "Port Said", "country": "EG", "lat": 31.26, "lon": 32.30},
]

CITIES = [
    {"name": "Cairo", "lat": 30.04, "lon": 31.24},
    {"name": "Giza", "lat": 30.01, "lon": 31.21},
    {"name": "Mansoura", "lat": 31.04, "lon": 31.38},
    {"name": "Tanta", "lat": 30.79, "lon": 31.00},
    {"name": "Asyut", "lat": 27.18, "lon": 31.18},
]

# delay_factor > 1 means the carrier is slower; speed in km/h while moving
CARRIERS = [
    {"carrier_id": "C1", "name": "FastLine", "delay_factor": 0.9, "speed_kmh": 62},
    {"carrier_id": "C2", "name": "NileCargo", "delay_factor": 1.0, "speed_kmh": 55},
    {"carrier_id": "C3", "name": "DeltaFreight", "delay_factor": 1.2, "speed_kmh": 50},
    {"carrier_id": "C4", "name": "SlowWave", "delay_factor": 1.5, "speed_kmh": 42},
]

# ---- chaos knobs -----------------------------------------------------------
LATE_EVENT_PROB = 0.05     # status event arrives hours late (event_time << ingest_time)
STUCK_PROB = 0.08          # shipment sits > 48h in one status
SILENT_PROB = 0.06         # GPS goes quiet for > 6h
PING_EVERY_HOURS = 2


def isoz(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def build_shipment(i, start, rng):
    carrier = rng.choice(CARRIERS)
    origin = rng.choice(PORTS)
    dest = rng.choice(CITIES)
    promised = start + timedelta(days=rng.randint(4, 8))
    return {
        "shipment_id": f"S{i:06d}",
        "origin_port_id": origin["port_id"],
        "origin": origin["name"],
        "origin_lat": origin["lat"],
        "origin_lon": origin["lon"],
        "destination": dest["name"],
        "dest_lat": dest["lat"],
        "dest_lon": dest["lon"],
        "carrier_id": carrier["carrier_id"],
        "lane": f'{origin["name"]}-{dest["name"]}',
        "pickup_time": isoz(start),
        "promised_date": isoz(promised),
    }


def build_timeline(shp, rng):
    """Return (list of (status, event_time), stuck_status or None)."""
    carrier = next(c for c in CARRIERS if c["carrier_id"] == shp["carrier_id"])
    f = carrier["delay_factor"]
    t = datetime.strptime(shp["pickup_time"], "%Y-%m-%dT%H:%M:%SZ")
    # hours spent BEFORE reaching the next status
    gaps = [(4, 12), (6, 30), (8, 36), (6, 20)]
    stuck_at = rng.randrange(1, 4) if rng.random() < STUCK_PROB else None
    timeline = [("picked_up", t)]
    stuck_status = None
    for idx, (lo, hi) in enumerate(gaps, start=1):
        hours = rng.uniform(lo, hi) * f
        if stuck_at == idx:
            hours += rng.uniform(50, 80)        # stuck more than 48h
            stuck_status = STATUSES[idx - 1]
        t = t + timedelta(hours=hours)
        timeline.append((STATUSES[idx], t))
    return timeline, stuck_status


def build_pings(shp, timeline, rng):
    carrier = next(c for c in CARRIERS if c["carrier_id"] == shp["carrier_id"])
    start, end = timeline[0][1], timeline[-1][1]
    total = (end - start).total_seconds()
    silent_from = silent_to = None
    if rng.random() < SILENT_PROB and total > 30 * 3600:
        silent_from = start + timedelta(seconds=total * rng.uniform(0.2, 0.6))
        silent_to = silent_from + timedelta(hours=rng.uniform(7, 12))
    pings, t = [], start
    while t <= end:
        if not (silent_from and silent_from <= t <= silent_to):
            frac = (t - start).total_seconds() / total
            # find the current status to decide if the truck is moving
            current = [s for s, et in timeline if et <= t][-1]
            moving = current in ("picked_up", "out_for_delivery")
            speed = max(0.0, rng.gauss(carrier["speed_kmh"], 6)) if moving else 0.0
            pings.append({
                "shipment_id": shp["shipment_id"],
                "lat": round(shp["origin_lat"] + (shp["dest_lat"] - shp["origin_lat"]) * frac + rng.gauss(0, 0.01), 5),
                "lon": round(shp["origin_lon"] + (shp["dest_lon"] - shp["origin_lon"]) * frac + rng.gauss(0, 0.01), 5),
                "speed_kmh": round(speed, 1),
                "ping_time": isoz(t),
                "ingest_time": isoz(t + timedelta(seconds=rng.randint(1, 30))),
            })
        t += timedelta(hours=PING_EVERY_HOURS)
    return pings


def build_events(shp, timeline, rng):
    events = []
    for status, et in timeline:
        if rng.random() < LATE_EVENT_PROB:
            delay = timedelta(hours=rng.uniform(1, 48))       # late event
        else:
            delay = timedelta(seconds=rng.randint(1, 30))
        events.append({
            "shipment_id": shp["shipment_id"],
            "status": status,
            "event_time": isoz(et),
            "ingest_time": isoz(et + delay),
        })
    return events


def generate(n, window_start, window_end, rng):
    shipments, events, pings = [], [], []
    span = (window_end - window_start).total_seconds()
    for i in range(1, n + 1):
        start = window_start + timedelta(seconds=rng.uniform(0, span))
        shp = build_shipment(i, start, rng)
        timeline, _ = build_timeline(shp, rng)
        shipments.append(shp)
        events += build_events(shp, timeline, rng)
        pings += build_pings(shp, timeline, rng)
    return shipments, events, pings


# ---- writing ---------------------------------------------------------------
def write_atomic(path, records):
    """Write to a hidden temp file then rename, so Spark never reads half a file."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / f".tmp_{path.name}"
    with open(tmp, "w", encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")
    os.replace(tmp, path)


def write_batch(out, shipments):
    write_atomic(Path(out) / "batch" / "carriers.json",
                 [{k: c[k] for k in ("carrier_id", "name")} for c in CARRIERS])
    write_atomic(Path(out) / "batch" / "ports.json", PORTS)
    write_atomic(Path(out) / "batch" / "shipments.json", shipments)


def write_chunks(out, kind, prefix, records, chunk=200, start_seq=1):
    records = sorted(records, key=lambda r: r["ingest_time"])
    seq = start_seq
    for i in range(0, len(records), chunk):
        write_atomic(Path(out) / "stream" / kind / f"{prefix}_{seq:06d}.json", records[i:i + chunk])
        seq += 1
    return seq


# ---- modes -----------------------------------------------------------------
def run_static(args, rng):
    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    days = args.days if args.mode == "backfill" else 5
    shipments, events, pings = generate(args.shipments, now - timedelta(days=days), now, rng)
    # only keep what would already have arrived by "now" (some shipments stay in progress)
    events = [e for e in events if e["ingest_time"] <= isoz(now)]
    pings = [p for p in pings if p["ingest_time"] <= isoz(now)]
    write_batch(args.out, shipments)
    write_chunks(args.out, "events", "events", events)
    write_chunks(args.out, "gps", "gps", pings)
    print(f"{args.mode}: {len(shipments)} shipments, {len(events)} events, {len(pings)} pings -> {args.out}/")


def run_stream(args, rng):
    """Replay a simulated clock: each tick, release records whose ingest_time has been reached."""
    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    window_start = now - timedelta(hours=args.spread_hours)
    shipments, events, pings = generate(args.shipments, window_start, now, rng)
    write_batch(args.out, shipments)
    pending = sorted(events + pings, key=lambda r: r["ingest_time"])
    sim_now = window_start
    ev_seq = gps_seq = 1
    print(f"stream: {len(pending)} records to release. Ctrl+C to stop.")
    while pending:
        sim_now += timedelta(seconds=args.speed * args.interval)
        cut = isoz(sim_now)
        ready = [r for r in pending if r["ingest_time"] <= cut]
        pending = pending[len(ready):]
        ev = [r for r in ready if "status" in r]
        gp = [r for r in ready if "ping_time" in r]
        if ev:
            write_atomic(Path(args.out) / "stream" / "events" / f"events_{ev_seq:06d}.json", ev)
            ev_seq += 1
        if gp:
            write_atomic(Path(args.out) / "stream" / "gps" / f"gps_{gps_seq:06d}.json", gp)
            gps_seq += 1
        print(f"sim_time={cut}  events={len(ev)}  pings={len(gp)}  left={len(pending)}")
        time.sleep(args.interval)
    print("stream: done.")


def main():
    p = argparse.ArgumentParser(description="Logistics Control Tower simulator")
    p.add_argument("--mode", choices=["sample", "backfill", "stream"], default="sample")
    p.add_argument("--shipments", type=int, default=None)
    p.add_argument("--days", type=int, default=90, help="history length for backfill")
    p.add_argument("--out", default="data")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--interval", type=float, default=5, help="stream: real seconds between files")
    p.add_argument("--speed", type=float, default=600, help="stream: simulated seconds per real second")
    p.add_argument("--spread-hours", type=int, default=48, help="stream: shipments start within the last N sim hours")
    args = p.parse_args()
    if args.shipments is None:
        args.shipments = {"sample": 20, "backfill": 3000, "stream": 100}[args.mode]
    rng = random.Random(args.seed)
    if args.mode == "stream":
        run_stream(args, rng)
    else:
        run_static(args, rng)


if __name__ == "__main__":
    main()
