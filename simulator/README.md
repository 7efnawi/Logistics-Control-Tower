# Simulator

Generates synthetic logistics data: shipments, status events and GPS pings, with deliberate chaos
(late events, stuck shipments, silent GPS). Needs only Python 3.9+ (no extra packages).
The seed is fixed (`--seed 42`), so everyone gets identical data.

## Run

```
python generate.py --mode sample                  # 20 shipments, for quick tests
python generate.py --mode backfill                # 3000 shipments over 90 days (history for scorecards)
python generate.py --mode stream                  # drops small files every few seconds (Ctrl+C to stop)
```

Useful options: `--shipments N`, `--days N` (backfill), `--out FOLDER`,
`--interval SECONDS` and `--speed SIM_SECONDS_PER_REAL_SECOND` (stream).

Tip: use a different `--out` per mode (or delete the folder first) so runs do not mix.

## Output

JSON Lines (one JSON record per line, files end with `.json`):

```
<out>/batch/carriers.json
<out>/batch/ports.json
<out>/batch/shipments.json
<out>/stream/events/events_000001.json     status events
<out>/stream/gps/gps_000001.json           GPS pings
```

Files are written under a hidden temp name and then renamed, so Spark never reads a half-written file.

## Schemas

| File | Columns |
|---|---|
| carriers | carrier_id, name |
| ports | port_id, name, country, lat, lon |
| shipments | shipment_id, origin_port_id, origin, origin_lat, origin_lon, destination, dest_lat, dest_lon, carrier_id, lane, pickup_time, promised_date |
| events | shipment_id, status, event_time, ingest_time |
| gps | shipment_id, lat, lon, speed_kmh, ping_time, ingest_time |

Status order: `picked_up -> at_port -> customs -> out_for_delivery -> delivered`.
All timestamps are UTC ISO strings (`2026-11-27T10:00:00Z`).

## Chaos built in

- ~5% of status events arrive late (`ingest_time` is 1-48h after `event_time`), so they can come after a newer status.
- ~8% of shipments are stuck more than 48h in one status.
- ~6% of shipments have a GPS gap of 7-12h (silent).
- Carriers differ in speed and delay (SlowWave is the slowest), so scorecards show real differences.

## Stream mode note

Timestamps in stream mode are simulated time (accelerated by `--speed`), not your real clock.
Always use `event_time`, not `ingest_time`, for business logic.
