# AirportEye POC - passenger queue feed for the Esri and GeoKno teams

This document describes what the AirportEye POC publishes to Kafka for the Esri and GeoKno teams, how to run it on a
laptop, and what the Esri side needs to configure. All data is **synthetic**.

> **Two ways to produce this feed.** `scripts/local_poc.sh demo` / `pax` measure it from the simulated
> passengers in `docs/PASSENGER_TRACKING.md` (recommended: it agrees with the position, zone and journey
> feeds, and `service_channels` is the number of lanes **open**). `scripts/local_poc.sh stream` / `files`
> run the older standalone queue simulation described below. The record format is the same.

## 1. What is sent

| | |
|---|---|
| Kafka topic | `airport-eye.poc.esri.passenger.touchpoint` |
| Message key | `HYD:<touchpoint_id>` (so each touchpoint's records stay in order) |
| Message value | one flat JSON object (UTF-8), no nesting |
| Cadence | one record per touchpoint per minute, all touchpoints in the same minute |
| Volume | 16 touchpoints = 16 records per minute |
| Airport | Hyderabad (`HYD`), terminal `HYD-MAIN` |

### The four numbers that were asked for

| Asked for | Field | Notes |
|---|---|---|
| Current queue length | `queue_length` | People waiting at the end of the minute. |
| Current wait time | `estimated_wait_seconds` and `avg_wait_time_seconds` | Estimated = what someone joining now should expect. Average = measured wait of the people who finished in the last minute (null if nobody did). |
| Current processing time | `avg_processing_time_seconds` | Measured service time of the people who finished in the last minute (null if nobody did). |
| Total throughput, midnight to now | `throughput_since_midnight` | People served from airport-local midnight (Asia/Kolkata) to the end of this minute. Restarts at 00:00 local; `service_day` says which date it belongs to. |

Everything else in the record (targets, status, location) is extra context. All field names are in the table below.

### Touchpoints in scope (16)

| Type | Touchpoints |
|---|---|
| entry | `SYN-HYD-ENTRY`, `SYN-HYD-DIGIYATRA-ENTRY`, `SYN-HYD-PRM-ENTRY` |
| check_in | `SYN-HYD-CHECKIN-DOM`, `SYN-HYD-CHECKIN-INT`, `SYN-HYD-CITY-CHECKIN`, `SYN-HYD-GA-CHECK-IN` |
| security | `SYN-HYD-SECURITY-DOM`, `SYN-HYD-SECURITY-INT`, `SYN-HYD-SECURITY-PRM`, `SYN-HYD-GA-SECURITY` |
| boarding | `SYN-HYD-BOARDING-DOM`, `SYN-HYD-BOARDING-INT` |
| immigration | `SYN-HYD-IMMIGRATION`, `SYN-HYD-IMMIGRATION-EGATE`, `SYN-HYD-GA-IMMIGRATION` |

The other touchpoints in `config/hyderabad_touchpoints.json` (bag drop, self check-in kiosks,
emigration, customs, transfer, baggage reclaim, information desks, exit) are switched off for now.
They stay in the inventory; the switch is `include_types` in `config/passenger_touchpoint.json`.

### Record fields

| Field | Type | Meaning |
|---|---|---|
| `entity_id` | string | Touchpoint ID. **Use as the track/ID field.** |
| `event_time_epoch_ms` | long | End of the one-minute window, UTC epoch milliseconds. **Use as the time field** (no date format needed). |
| `longitude`, `latitude` | double | WGS84 (EPSG:4326). **Use as X / Y.** Inside the real terminal outline; interior positions are schematic, see section 5. |
| `floor` | string | Floor label (`L1` departures, `L0` arrivals). |
| `elevation_m` | double | Floor height above the ground floor in metres (0 or 8): Z for 3D scenes. |
| `touchpoint_type` | string | entry, check_in, security, boarding, immigration |
| `zone_id` | string | Terminal zone the touchpoint is in. |
| `queue_length` | int | People waiting at the end of the window (not counting those being served). |
| `passengers_in_service` | int | People currently being served. |
| `processed_passengers` | int | Services completed during the minute. |
| `throughput_since_midnight` | int | Services completed from airport-local midnight to `window_end`. |
| `service_day` | string | Airport-local date (`YYYY-MM-DD`) that the since-midnight total belongs to. |
| `avg_wait_time_seconds` | double or null | Mean wait of those completed in the minute; null if none completed. |
| `avg_processing_time_seconds` | double or null | Mean service time of those completed; null if none completed. |
| `touchpoint_name` | string | Readable name, e.g. "Domestic airline check-in counters". |
| `flow` | string | domestic, international, priority, digiyatra, city_side, general_aviation or all. |
| `service_channels` | int | Parallel service slots (counters, lanes, e-gates) at this touchpoint. |
| `utilization_pct` | double | `passengers_in_service` / `service_channels` x 100. |
| `throughput_per_hour` | double | `processed_passengers` x 60 (people per hour at the current rate). |
| `estimated_wait_seconds` | double | What a person joining now should expect: queue length / capacity. |
| `p95_wait_seconds` | double or null | 95th percentile of waits over the last 30 minutes; null until 5 samples exist. |
| `target_wait_seconds` | double or null | **Operational target** for waiting (see section 6). Null where no target exists. |
| `aera_existing_wait_seconds` | double or null | AERA's current guideline (95% of passengers). |
| `aera_proposed_wait_seconds` | double or null | AERA's August 2025 **proposal** for Category A airports. Not enacted. |
| `target_basis` | string | `recommended` (in the guidance), `assumed_from_similar` (we reused a similar touchpoint's target) or `none`. |
| `queue_limit` | int or null | Queue length at which the wait target would be reached: capacity per minute x target minutes. |
| `status` | string | `normal`, `busy` (60% to 100% of target), `congested` (target reached), `no_target`. |
| `sla_breached` | bool or null | True when estimated or 95th-percentile wait reaches the operational target. |
| `window_start`, `window_end` | ISO-8601 UTC string | The minute the numbers describe. |
| `event_time`, `ingestion_time` | ISO-8601 UTC string | Same instants as strings; `*_epoch_ms` twins exist. |
| `event_id`, `event_type`, `schema_version`, `airport_id`, `terminal_id`, `source_system`, `sequence_no` | | Envelope metadata. |

### Sample record

```json
{
  "event_id": "9302bf50-7b06-504f-ad02-98b206e4f4a4",
  "event_type": "passenger.touchpoint",
  "schema_version": "1.0.0",
  "airport_id": "HYD",
  "terminal_id": "HYD-MAIN",
  "source_system": "synthetic-touchpoint-generator",
  "entity_id": "SYN-HYD-CHECKIN-DOM",
  "sequence_no": 29,
  "event_time": "2026-09-29T03:00:00Z",
  "ingestion_time": "2026-09-29T03:00:00Z",
  "event_time_epoch_ms": 1790650800000,
  "ingestion_time_epoch_ms": 1790650800000,
  "touchpoint_id": "SYN-HYD-CHECKIN-DOM",
  "touchpoint_type": "check_in",
  "zone_id": "SYN-HYD-ZONE-CHECKIN",
  "window_start": "2026-09-29T02:59:00Z",
  "window_end": "2026-09-29T03:00:00Z",
  "queue_length": 0,
  "passengers_in_service": 19,
  "processed_passengers": 12,
  "avg_wait_time_seconds": 1.138,
  "avg_processing_time_seconds": 64.061,
  "throughput_since_midnight": 7702,
  "service_day": "2026-09-29",
  "touchpoint_name": "Domestic airline check-in counters",
  "flow": "domestic",
  "service_channels": 20,
  "utilization_pct": 95.0,
  "throughput_per_hour": 720.0,
  "estimated_wait_seconds": 0.0,
  "p95_wait_seconds": 72.4,
  "target_wait_seconds": 600.0,
  "aera_existing_wait_seconds": 1200.0,
  "aera_proposed_wait_seconds": 1200.0,
  "target_basis": "recommended",
  "queue_limit": 165,
  "status": "normal",
  "sla_breached": false,
  "longitude": 78.4288145,
  "latitude": 17.238996,
  "floor": "L0"
}
```

## 2. Run it on a laptop

You need Docker Desktop (running) and Python 3.11 or newer.

```bash
cd digital-twin-poc-local-esri
scripts/local_poc.sh setup      # one time: creates .venv and installs the project
scripts/local_poc.sh demo       # starts Kafka, the map viewer and the per-minute publisher
```

`demo` opens a map at <http://127.0.0.1:8080> and publishes one batch per minute until you press
Ctrl-C. The first batch goes out immediately. To see it move faster, add `--speed 60` (one batch per
second; event times then run ahead of the real clock, so use this for looking, not for Esri ingest).

Other commands:

```bash
scripts/local_poc.sh test            # unit tests + a publish-and-read-back check on the real broker
scripts/local_poc.sh kafka-up        # just the broker, on localhost:9092
scripts/local_poc.sh stream          # just the publisher
scripts/local_poc.sh viewer          # just the map
scripts/local_poc.sh kafka-ui        # browser UI for topics and messages at http://127.0.0.1:8081
scripts/local_poc.sh kafka-down -v   # stop Kafka (and the UI) and delete its data
```

In the Kafka UI, open **Topics > `airport-eye.poc.esri.passenger.touchpoint` > Messages** to see each
record as it arrives; click a row to see every field.

No broker handy? `python scripts/map_viewer.py --file generated_data/passenger_touchpoint.jsonl`
replays the generated hour on the map without Kafka.

Look at the raw messages yourself:

```bash
docker compose --profile kafka exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic airport-eye.poc.esri.passenger.touchpoint --from-beginning
```

Queue data as CSV, GeoJSON and JSON files, no Kafka or Docker needed (see `HOW_TO_USE.md`):

```bash
scripts/local_poc.sh files --start 2026-10-09T08:00:00+05:30 --hours 4   # a time range in one go
scripts/local_poc.sh files --live                                         # rewrite output/passenger_queue_latest.* every minute
```

Generate the raw event data (writes `generated_data/passenger_touchpoint.jsonl`, one hour, 960 records):

```bash
.venv/bin/airport-eye generate passenger_touchpoint
.venv/bin/airport-eye publish generated_data/passenger_touchpoint.jsonl --format esri --dry-run --limit 3
# the same records as flat Esri JSON in a file (what the Kafka topic carries), for sharing
.venv/bin/airport-eye export generated_data/passenger_touchpoint.jsonl --out esri_sample.jsonl
```

## 3. Connecting Esri

Esri's side must be able to reach the Kafka broker. A broker on a laptop (`localhost:9092`) is only
reachable from that laptop. For ArcGIS Velocity (hosted in the cloud) or a GeoEvent Server on another
machine, the broker needs an address they can reach, plus matching security settings. Set
`KAFKA_BOOTSTRAP_SERVERS` and the `KAFKA_SECURITY_PROTOCOL` / `KAFKA_SASL_*` variables (see
`.env.example`) when publishing to a shared or cloud broker.

Suggested feed settings (please confirm against your product version):

- Format JSON, one object per message, no root node needed.
- Track ID `entity_id`; time `event_time_epoch_ms` (epoch milliseconds); geometry X `longitude`, Y
  `latitude`, WGS84.
- Nothing needs flattening: records are already flat. Fields that can be null: `avg_wait_time_seconds`,
  `avg_processing_time_seconds`.

## 4. How it was checked

- 44 unit tests covering the publisher, the file output, the Esri record shape, the touchpoint filter, the per-minute pacing and the target calculations (limits, estimated and 95th-percentile wait, status, breach flag).
- A wire-level run against Kafka's in-process mock broker: 48 records over 3 batches, none failed.
- Real Kafka broker (Apache Kafka 3.9.1 in Docker): `scripts/local_poc.sh test` published 200 records and
  read all 200 back; a live `demo` run delivered more than 21,000 records with none failed. The map viewer
  and the Kafka UI showed the feed live.
- **Not yet run:** against an actual ArcGIS Velocity or GeoEvent feed.

## 5. Known limits and questions for Esri

- **Locations are schematic inside the real building.** The terminal outline and gates come from
  OpenStreetMap; touchpoint positions, zones and floors are drawn inside it
  (`config/terminal_layout.json`, see `docs/PASSENGER_TRACKING.md`). Exact positions need GHIAL's
  floor plans or an Esri indoor/BIM layer.
- **Is this the schema you want?** Field names and the epoch-millisecond time follow Esri's public
  documentation, not an agreed feed definition. Tell us any renames or extra fields.
- **Queues are independent.** Each touchpoint in this feed simulates its own arrivals. The passenger
  tracking feed (`docs/PASSENGER_TRACKING.md`) does connect them, with people moving from entry to
  check-in to security; the queue feed is not yet derived from it.
- The sample file is one hour (08:00 to 09:00 IST). Queues are simulated from local midnight, so totals in the sample already include the earlier hours.

## 6. Targets and how the queue attributes are calculated

Targets live in `config/service_targets.json`. Operational targets are starting points taken from the
operations guidance (wait = joining the queue to starting service; applies to 95% of passengers). They
are **not** published Hyderabad SLAs. AERA values are kept in their own fields and never mixed with
operating targets. As of 8 Oct 2026 no final notification enacting AERA's August 2025 proposal was found.
Hyderabad's concession agreement uses an annual passenger survey (3.5/5), not these waiting limits.

| Touchpoint class | Operational target | AERA existing | AERA proposed |
|---|---|---|---|
| Entry, manual | 3 min | none | 10 min |
| Entry, Digi Yatra | 1 min | none | 5 min |
| Check-in, economy (staffed) | 10 min | 20 min | 20 min |
| Security | 5 min | 5 min | 10 min |
| Arrival immigration, manual | 7 min | 10 min | 12 min |
| Boarding gate | **no target in the guidance** | none | none |

Calculations (per touchpoint, per minute):

- capacity per minute = `service_channels` x 60 / mean service seconds
- `queue_limit` = capacity per minute x target wait in minutes (the guidance's formula; counters only
  multiply when they feed one shared queue)
- `estimated_wait_seconds` = `queue_length` / capacity per minute x 60
- `p95_wait_seconds` = 95th percentile (nearest rank) over the last 30 minutes of completed services
- ratio = larger of estimated wait and 95th-percentile wait, divided by the target; `status` is
  `normal` below 0.6, `busy` below 1, `congested` at 1 or more; `sla_breached` is true from 1 up

Touchpoints whose target is an assumption (priority lanes, the Digi Yatra-style e-gate, city-side and
general-aviation points, and the priority entry) are marked `target_basis = assumed_from_similar`.

**Open item: calibration.** The guidance's processing-time assumptions are longer than the simulator's
current service times (for example staffed domestic check-in 2-3 min vs 45-100 s now; international
check-in 3-5 min vs 60-150 s; manual entry 15-30 s vs 8-20 s). Arrival rates, counter counts and service
times have to be calibrated together: with the longer times and today's counter counts, check-in and entry
would be overloaded. Nothing in the inventory was changed. Use RGIA observations to set all three.

## 7. Next

1. 100,000 temperature points, one reading per minute.
2. Passenger positions: done, see `docs/PASSENGER_TRACKING.md` (topic
   `airport-eye.poc.esri.passenger.position`). Next there: derive the queue feed from the tracked
   passengers, and swap the schematic interior for GHIAL floor plans.
