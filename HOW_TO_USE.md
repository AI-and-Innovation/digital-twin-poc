# AirportEye POC - How to use

A passenger-queue simulation for Hyderabad airport. It writes the queue data as CSV, GeoJSON and JSON
files that load straight into ArcGIS, and can also publish the same records live to Kafka for ArcGIS
Velocity / GeoEvent Server. Every minute it sends one record for each of 16
touchpoints (entry, check-in, security, boarding, immigration) with the queue length, the wait time,
the processing time, the throughput since midnight, the target status and a map location.

It also tracks synthetic passengers through the terminal every 5 seconds (section 4b and
`docs/PASSENGER_TRACKING.md`).

**All data is synthetic.** The terminal outline is real (OpenStreetMap); positions inside it are schematic.

## 1. What is in this package

| Path | What it is |
|---|---|
| `HOW_TO_USE.md` | This guide |
| `docs/ESRI_SETUP.md` | Full reference: every field, a sample record, Esri feed settings, targets and calculations |
| `docs/PASSENGER_TRACKING.md` | Passenger tracking: where coordinates come from, how passengers move, the position feed |
| `sample_output/` | Ready-made data to inspect without running anything: one hour, 960 records, as CSV, JSONL and a readable JSON sample |
| `scripts/local_poc.sh` | One-command runner (setup, queue files, Kafka demo, Kafka UI, tests) |
| `src/airport_eye/` | The simulator and the Kafka publisher (Python) |
| `config/` | Touchpoints, arrival rates, service times, wait targets, terminal layout, passenger tracking settings |
| `docker-compose.yml` | Local Kafka broker and Kafka UI |

## 2. Just want to look at the data?

Open `sample_output/passenger_queue_esri.csv` in Excel, or `sample_output/passenger_queue_sample.json`
in any text editor. Each row or object is one record (one touchpoint, one minute).

## 3. Generate passenger queue files (no Kafka, no Docker)

**You need:** Python 3.11 or newer, on macOS or Linux (on Windows, use WSL2).

Open a terminal in the unzipped folder:

```bash
cd digital-twin-poc-local-esri
chmod +x scripts/local_poc.sh      # only needed if the zip tool dropped the permission
scripts/local_poc.sh setup         # one time: creates .venv and installs the project
```

**A time range in one go:**

```bash
scripts/local_poc.sh files                                                   # the default hour
scripts/local_poc.sh files --start 2026-10-09T08:00:00+05:30 --hours 4       # any start and length
```

This writes `output/passenger_queue_<start>.csv`, `.geojson` and `.json`, with one record per touchpoint
per minute.

**Live, every minute:**

```bash
scripts/local_poc.sh files --live
```

Every minute this rewrites `output/passenger_queue_latest.csv` / `.geojson` / `.json` (16 rows: the current
minute for each touchpoint) and adds the same rows to `output/passenger_queue_history.csv`. Press Ctrl-C to
stop. The latest files are swapped in whole, so a reader never sees a half-written file. Add
`--speed 60` for a quick look (one minute per second), and `--out <folder>` to write somewhere else.
A new `--live` run starts a fresh history file.

**Load into ArcGIS:** upload the GeoJSON, or the CSV with Longitude = `longitude` and Latitude = `latitude`
(WGS84), as a hosted feature layer. In ArcGIS Pro, use Add Data or XY Table To Point. For a time slider,
enable time on `window_end` (UTC).

## 4. Optional: live Kafka feed and map

**Also needs:** Docker Desktop running.

```bash
scripts/local_poc.sh demo          # starts Kafka, the dashboard, and the terminal simulation (all four feeds)
```

`demo` opens the dashboard at <http://127.0.0.1:8080> and publishes until you press Ctrl-C: passenger
positions every 5 s, and the 16 checkpoint queue records, zone occupancy and journey KPIs every minute.
For a quick look, run `scripts/local_poc.sh demo --speed 12`. Don't use `--speed` when Esri is ingesting,
because event times then run ahead of the clock.

## 4b. Terminal dashboard and passenger tracking

```bash
scripts/local_poc.sh map                        # dashboard with the simulation built in (no Kafka, no Docker)
scripts/local_poc.sh map --scenario peak        # the same, busy at any hour (queues and alerts for a demo)
scripts/local_poc.sh pax-files --minutes 15     # 15 minutes of all four feeds to output/ (no Kafka)
scripts/local_poc.sh pax-files --live           # rewrite output/*_latest.* continuously
scripts/local_poc.sh pax                        # all four feeds live to Kafka
```

One simulation of Terminal 1 produces four feeds: passenger positions every 5 s, and every minute the
checkpoint queues, zone occupancy and journey times. They all come from the same passengers, so they
agree. `demo` runs this simulation (it replaces the separate queue publisher). The dashboard at
<http://127.0.0.1:8080> shows terminal KPIs, alerts, checkpoints against their targets, zone density, a
heatmap, one floor at a time, lets you follow any passenger, and replays the last 90 minutes.
Details and every field: `docs/PASSENGER_TRACKING.md`.

### See the live data

- **Map:** <http://127.0.0.1:8080>. The list on the left shows each touchpoint's queue length and estimated
  wait, longest queue first. On the map, a bigger circle means a longer queue. The colour is the status:
  green normal, amber busy, red over target. Click a circle for all its values and a 60-minute queue chart.
- **Kafka UI:** run `scripts/local_poc.sh kafka-ui` in a second terminal, then open <http://127.0.0.1:8081>
  and go to **Topics > `airport-eye.poc.esri.passenger.touchpoint` > Messages**. Click a row to see every field.
- **Command line:**
  ```bash
  docker compose --profile kafka exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic airport-eye.poc.esri.passenger.touchpoint
  ```

## 5. How the Kafka feed works

```
Queue simulation  ->  flat Esri JSON  ->  Kafka producer  ->  Kafka topic  ->  Map viewer / Esri
(generators/        (esri.py)          (publishers/       airport-eye.poc.esri.passenger.touchpoint
 passenger_touchpoint.py)               kafka.py)
```

| Setting | Value |
|---|---|
| Broker (local) | `localhost:9092` (Apache Kafka 3.9.1 in Docker) |
| Topic | `airport-eye.poc.esri.passenger.touchpoint` (12 partitions) |
| Message key | `HYD:<touchpoint_id>`, so each touchpoint's records stay in order |
| Message value | one flat JSON object, UTF-8, 41 fields, no nesting |
| Rate | 16 records per minute |
| Delivery | `acks=all`, idempotent producer, lz4 compression |

## 6. Connecting Esri

Suggested feed settings (please confirm for your product version):

- **Format:** JSON, one object per message, nothing to flatten
- **Track ID:** `entity_id`
- **Time:** `event_time_epoch_ms` (epoch milliseconds, UTC)
- **Geometry:** X = `longitude`, Y = `latitude`, WGS84 (EPSG:4326)
- **Can be null:** `avg_wait_time_seconds`, `avg_processing_time_seconds`, `p95_wait_seconds`,
  `target_wait_seconds`, `queue_limit`, `sla_breached`

A broker on a laptop is only reachable from that laptop. To feed ArcGIS Velocity or a GeoEvent Server
elsewhere, publish to a broker they can reach:

```bash
export KAFKA_BOOTSTRAP_SERVERS=<host:port>
export KAFKA_SECURITY_PROTOCOL=SASL_SSL          # if the broker needs it
export KAFKA_SASL_MECHANISM=PLAIN                # and these, if it needs a login
export KAFKA_SASL_USERNAME=<user>
export KAFKA_SASL_PASSWORD=<password>
scripts/local_poc.sh stream
```

See `docs/ESRI_SETUP.md` for every field and how the targets and wait figures are calculated.

## 7. Stop

Press Ctrl-C in the `demo` terminal, then:

```bash
scripts/local_poc.sh kafka-down        # stop Kafka and the Kafka UI
scripts/local_poc.sh kafka-down -v     # same, and delete the stored messages
```

## 8. If something goes wrong

| Problem | Fix |
|---|---|
| "Python 3.11 or newer is required" | Install Python 3.12 (macOS: `brew install python@3.12`) |
| "Docker is installed but not running" | Start Docker Desktop and retry |
| Port 8080, 8081 or 9092 already in use | Stop whatever is using it, or run the viewer with `scripts/local_poc.sh viewer --port 8090` |
| Log lines like `Connect to ipv6#[::1]:9092 failed` | Harmless: the client tries IPv6 first, then connects over IPv4 |
| Map shows no data | Check the `demo` terminal shows `batch N: 16 records (... failed 0)` lines |
| `--start` rejected | Give a UTC offset, e.g. `2026-10-09T08:00:00+05:30` |

## 9. Checked so far

- 44 unit tests pass (including the file output), and `scripts/local_poc.sh test` publishes 200 records to a real Kafka broker and reads all 200 back.
- A live run delivered more than 21,000 records with none failed. The map and the Kafka UI showed them live.
- Not yet tested against an actual ArcGIS Velocity or GeoEvent feed. That is the next step with the Esri team.
