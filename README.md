# Airport Eye POC synthetic-data scaffold

Python starter contracts for comparing Unreal and ArcGIS as airport digital twin
visualization clients. The intended streams cover facility/MEP telemetry, passenger
tracking and heatmaps, AODB, RMS, A-CDM, and aircraft movements. **Passenger
touchpoint queue generation is implemented; other generators remain scaffolds.**
No realistic trajectories, sensor dynamics, Kafka networking, or visualization
adapters are implemented. The queue model is synthetic and not airport-calibrated.

## Generate passenger touchpoint data

```powershell
python -m airport_eye generate passenger_touchpoint --config config/passenger_touchpoint.json
```

After installation (see below), this writes `generated_data/passenger_touchpoint.jsonl`:
one complete JSON event per line, one file for this template across all touchpoints.
Other template files will be produced when their generators are implemented.
The default run covers 08:00–09:00 India time on 29 September 2026 in 60-second
windows: 40 enabled touchpoints × 60 windows = 2,400 events. This is offline
generation; it does not sleep for a minute between records. Reruns replace this
template's previous dataset atomically and leave other template files unchanged.

Edit these files:

- `config/hyderabad_touchpoints.json`: add, remove or modify touchpoint objects;
  `enabled: false` excludes an entry. IDs must be unique. `touchpoint_type` is an
  editable nonempty string, so adding a category does not require a code change.
  Each entry includes source references and a `simulation` profile with arrival
  rate/hour, parallel service channels, service duration range in seconds, and
  initial passenger count waiting before service begins.
- `config/passenger_touchpoint.json`: set `start_time`, `end_time`,
  `interval_seconds` (default 60), and `seed`. Both times require timezone offsets;
  output timestamps are UTC. Paths resolve relative to this configuration file.
  Duration must be positive and a whole multiple of the interval. The first event
  is at start + interval, the last at end; each measures `(window_start, window_end]`.
  This dedicated run config is independent of the broader `config/poc.json` scaffold.

The inventory covers passenger processing and assistance at Hyderabad RGIA,
including main-terminal departures, arrivals, transfers, published virtual
helpdesk locations, and general aviation. The editable source list records
official airport pages and the research date. Core references include
[RGIA facilities](https://www.hyderabad.aero/rgia),
[self-service check-in](https://www.hyderabad.aero/rgia/airport-facilities/self-check-in),
[passenger FAQs](https://www.hyderabad.aero/travellers/faqs),
[helpdesk locations](https://www.hyderabad.aero/rgia/airport-facilities/virtual-helpdesk),
and [general aviation](https://www.hyderabad.aero/rgia/airport-facilities/general-aviation).

**This is a public-source process inventory, not a verified exhaustive register
of individual lanes, gates or counters.** It has 44 entries: 40 enabled and four
unverified candidates disabled pending airport confirmation. Commercial amenities
and ground transport are outside its scope. Source pages do not establish every
physical location or domestic/international subdivision used here. All `SYN-`
IDs, zones, capacities and load profiles are synthetic assumptions. Replace or
expand process groups with the airport's physical asset register when available.

Each touchpoint has an independent FIFO queue, exponential interarrival times
(Poisson arrivals), and uniformly distributed service durations. Queue and service
states persist across windows. Wait/service averages use only passengers who
completed service in the current window; both are null for zero completions.
Initial passengers enter at the run start, so initial service slots fill immediately;
there is no pre-run warm-up. `ingestion_time` equals simulated `event_time` for
reproducible offline data, not measured wall-clock ingestion latency. Per-touchpoint
random streams preserve values when other entries are added, removed or reordered.
Baggage reclaim is a coarse service-delay approximation. Queues are not connected
passenger journeys; their arrival counts must not be summed as airport throughput.

## Quick start

Python 3.11+ is required. From the repository root in PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e .
.\.venv\Scripts\airport-eye.exe validate-config
.\.venv\Scripts\airport-eye.exe validate-samples
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
.\.venv\Scripts\airport-eye.exe generate passenger --config config/poc.json
```

The final command deliberately exits with code 2 and an unimplemented message.
`python -m airport_eye` also exposes the CLI. Validation commands return 0 on
success and 2 on invalid input. Config/sample paths are relative to the current
directory; pass explicit paths when running elsewhere. For source-only use set
`$env:PYTHONPATH = "src"` with Pydantic 2 installed.

## File tree

```text
config/poc.json                 Validated scale, update-rate and Kafka settings
config/hyderabad_touchpoints.json Editable Hyderabad touchpoint catalog and sources
config/passenger_touchpoint.json Touchpoint run dates, interval and paths
generated_data/passenger_touchpoint.jsonl Generated touchpoint events
samples/                        Complete envelope + payload JSON examples
  common_event_envelope.json     Same example as facility_telemetry.json
  asset_registry.json
  point_registry.json
  facility_telemetry.json
  passenger_touchpoint.json
  passenger_position.json
  passenger_aggregate.json
  aodb_flight.json
  rms_assignment.json
  acdm_milestone.json
  aircraft_position.json
src/airport_eye/
  models.py                     Pydantic envelope, spatial and payload contracts
  config.py                     Pydantic configuration and JSON loader
  cli.py, __main__.py            Validation commands and generate stub
  common.py                     Entity-key and nominal-load helpers
  spatial.py                    Local frame <-> WGS84, walkway graph, zone lookup
  generators/
    facility.py                 Registry/telemetry placeholder
    passenger.py                Passenger tracking: flights, journeys, walking, connected queues
    passenger_touchpoint.py     Stateful touchpoint queue generator
    airport_operations.py       AODB/RMS/A-CDM placeholder
    aircraft.py                 Aircraft movement placeholder
  publishers/kafka.py            Record preparation and transport placeholder
tests/test_contracts.py          Executable contract checks and future test slots
tests/test_passenger_touchpoint.py Queue, cadence, inventory and output checks
.env.example                    Future publisher environment variables
docker-compose.yml              Opt-in development Kafka template
pyproject.toml                  Package metadata, CLI, optional Kafka dependency
```

## Scale assumptions

`config/poc.json` defaults to 90 million annual passengers, 200,000 registered
telemetry points, 20,000 active passenger tracks at 1 Hz, and 80 aircraft movements
per hour. Change `registered_points` to 100,000 for the lower telemetry scenario,
or `active_tracks` to 10,000 for the lower passenger scenario. Flight presets are
40/60/80/100/120; `movements_per_hour` accepts any positive integer. A movement is
one arrival or one departure, not a full aircraft turnaround.

Annual throughput, peak arrivals per hour and active concurrent tracks are
independent inputs. The illustrative 20,000 peak arrivals/hour is not derived from
annual throughput or validated airport capacity. Later simulation must calibrate
dwell times, schedules, staffing, routes and queues against a specific airport.

At defaults, 200k points × 0.1 Hz × 100% active = 20k telemetry events/s, plus
20k passenger positions/s. Raising telemetry to 1 Hz produces 200k telemetry
events/s. This excludes touchpoints, aggregates, aircraft and operational events;
these numbers are configured demand, not measured throughput. Operational
snapshot rates are separate from event-driven A-CDM milestones. Config controls
are preparatory and are not yet consumed by generators.

## Contracts and relationships

Every example contains `event_id`, `event_type`, `schema_version`, `airport_id`,
`terminal_id`, `source_system`, `entity_id`, `event_time`, `ingestion_time`,
`sequence_no`, and `payload`. Terminal is required but nullable for airport-wide
events. UUID event IDs support later deduplication. Timestamps require offsets;
examples use UTC. `event_time` is simulation occurrence time; `ingestion_time` is
producer emission time. Future replay/time scaling must keep those clocks distinct.
Sequence numbers start at zero and are scoped to airport + event type + entity
within a simulation run; restart/replay semantics remain to be implemented.

`EventEnvelope.model_validate(...)` dispatches to the matching payload model and
checks its entity key. Unknown types and fields are rejected. Pydantic models are
the schema source of truth; an envelope-only JSON Schema export would not capture
the runtime payload dispatch, so consumers should validate with these models.

| Event type | Entity key |
| --- | --- |
| registry.asset | asset_id |
| registry.point, facility.telemetry | point_id |
| passenger.position | passenger_id |
| passenger.touchpoint | touchpoint_id |
| passenger.aggregate | zone_id |
| aodb.flight, acdm.milestone | flight_id |
| rms.assignment | assignment_id |
| aircraft.position | aircraft_id |

Join `point.asset_id -> asset.asset_id -> asset.bim_element_id`, then use the
asset's `zone_id` and `gis_location_id` for spatial association. Flight IDs link
passengers, AODB, resource assignments, milestones and aircraft. BIM IDs are
external references only: LOD 350 geometry is not generated here. Registry
referential integrity is tested for samples; a runtime registry/catalog validator
will be needed for generated datasets, including point value types and units.

Indoor x/y/z are metres in an explicit local coordinate frame, with floor, zone
and grid IDs. Outdoor lat/lon use WGS84 degrees with altitude in metres and an
explicit vertical datum. The near-zero outdoor coordinates are fictional fixture
locations and are not a calibrated conversion of the indoor examples. Grid
polygons and BIM/GIS transforms must be supplied later. Aggregate cells form a
disjoint zone snapshot at the window end; counts sum to the zone count and density
is count / area. These are not unique passenger counts over the window.

Passenger and PNR fields require `SYN-` IDs. There are no names, contact details,
passport numbers or real passenger identifiers. Samples are contract examples,
not a complete movement timeline. The common-envelope file duplicates the
telemetry example for documentation and should not be replayed as an extra event.

`passenger.position` tracks an identified synthetic passenger's physical location
at `event_time`: passenger ID, synthetic PNR, flight ID, indoor x/y/z, floor, zone,
grid, speed, heading and physical movement state (walking or stationary).

`passenger.touchpoint` reports operational metrics for a configured passenger
touchpoint, keyed by touchpoint ID. It contains no individual passenger
or flight identity. `queue_length` counts waiting passengers at `window_end`,
excluding `passengers_in_service`. `processed_passengers` counts service
completions in `(window_start, window_end]`. Average wait time measures queue entry
to service start; average processing time measures service start to completion,
both in seconds for that same completed-service cohort, including durations that
started before the window. These averages are historical measurements, not
predicted waits for the current queue. Both are null when there are no completions.
Touchpoint windows need not match the 1 Hz physical tracking rate.

This revises the initial unreleased 1.0.0 scaffold in place: touchpoint consumers
must now use touchpoint keys and metric payloads, and position payloads require
synthetic PNR IDs. Position and touchpoint sample counts are independent fixture
examples, not a complete shared simulated population.

## Kafka and later implementation

`KafkaPublisher.prepare()` returns topic, key and JSON bytes without I/O. Topics
are `<topic_prefix>.<event_type>`; keys are UTF-8 `<airport_id>:<entity_id>`.
This groups each entity within a topic partition. It does not provide ordering
across passenger touchpoint/position topics or across flight and aircraft topics.
Keep partition counts stable if consumers depend on partition ordering.

Publishing is implemented (see the section at the end). Kafka idempotence is a
producer setting, not an end-to-end exactly-once guarantee. Still future work:
a shared seeded scheduler across all streams, rate/latency/drop metrics, and
production TLS/SASL operations.

Docker Compose is a single-node KRaft broker for local use, with a `kafka`
profile, host-local port binding and a health check. `KAFKA_IMAGE_TAG` defaults
to 3.9.1. Start it with `docker compose --profile kafka up -d --wait kafka`, or
let `scripts/local_poc.sh` do it. No broker is started by Python or tests.

Contract tests run now. Explicitly skipped simulation/transport tests mark later
acceptance work; their skips must not be interpreted as successful load tests.

## Publishing to Kafka and the Esri feed format

`KafkaPublisher` now sends records (install the extra: `pip install -e ".[kafka]"`).
It creates the producer lazily, retries when the local queue is full, counts
delivery callbacks, and `flush()` raises if anything is undelivered or failed.
Broker settings come from `config/poc.json`; `KAFKA_BOOTSTRAP_SERVERS`,
`KAFKA_SECURITY_PROTOCOL` and `KAFKA_SASL_*` environment variables override them.

```bash
# inspect what would be sent (no network)
airport-eye publish generated_data/passenger_touchpoint.jsonl --format esri --dry-run --limit 3
# send to a broker
airport-eye publish generated_data/passenger_touchpoint.jsonl --bootstrap localhost:9092
airport-eye publish generated_data/passenger_touchpoint.jsonl --bootstrap localhost:9092 --format esri
# end-to-end check against a real broker (publishes, then consumes back)
python scripts/kafka_smoke_test.py --bootstrap localhost:9092 --format esri
```

`--format envelope` (default) keeps the canonical nested contract on
`<prefix>.<event_type>`. `--format esri` sends a flat record on
`<prefix>.esri.<event_type>`: payload fields hoisted to the top level (nested
objects joined with `_`), `event_time_epoch_ms` as the time field, `entity_id`
as the track ID, and `longitude`/`latitude` (WGS84) where a location resolves.
Touchpoint events have no coordinates, so they are located through
`config/terminal_layout.json`: the Terminal 1 outline and gates from
OpenStreetMap, with a **schematic** interior (zones, touchpoints, walkways) drawn
inside it. Passenger positions carry indoor x/y in terminal-local metres, which
are converted to WGS84 with the layout's frame. See
[docs/PASSENGER_TRACKING.md](docs/PASSENGER_TRACKING.md).

Tested: unit tests with a fake producer, and a wire-level run against
librdkafka's in-process mock broker (2,400 records, 0 failures). Not yet tested
against a real broker, nor against an actual ArcGIS Velocity/GeoEvent feed.

## Local end-to-end run for the Esri POC

`scripts/local_poc.sh setup` then `scripts/local_poc.sh demo` starts Kafka, a
live per-minute publisher (`airport-eye stream`) and a local map
(`scripts/map_viewer.py`), plus live passenger tracking (`airport-eye pax-stream`,
every 5 s). Scope is set in `config/passenger_touchpoint.json`
with `include_types` (entry, check-in, security, boarding, immigration). The
full hand-off, field table and Esri connection notes are in
[docs/ESRI_SETUP.md](docs/ESRI_SETUP.md).
