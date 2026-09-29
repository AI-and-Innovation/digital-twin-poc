# Airport Eye POC synthetic-data scaffold

Python starter contracts for comparing Unreal and ArcGIS as airport digital twin
visualization clients. The intended streams cover facility/MEP telemetry, passenger
tracking and heatmaps, AODB, RMS, A-CDM, and aircraft movements. **This is a scaffold,
not a simulator or a tested high-volume publisher.** No realistic trajectories,
queueing, sensor dynamics, Kafka networking, or visualization adapters are implemented.

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
  spatial.py                    Transform and route placeholders
  generators/
    facility.py                 Registry/telemetry placeholder
    passenger.py                Touchpoint, track and aggregate placeholder
    airport_operations.py       AODB/RMS/A-CDM placeholder
    aircraft.py                 Aircraft movement placeholder
  publishers/kafka.py            Record preparation and transport placeholder
tests/test_contracts.py          Executable contract checks and future test slots
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
| passenger.touchpoint, passenger.position | passenger_id |
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

## Kafka and later implementation

`KafkaPublisher.prepare()` returns topic, key and JSON bytes without I/O. Topics
are `<topic_prefix>.<event_type>`; keys are UTF-8 `<airport_id>:<entity_id>`.
This groups each entity within a topic partition. It does not provide ordering
across passenger touchpoint/position topics or across flight and aircraft topics.
Keep partition counts stable if consumers depend on partition ordering.

`publish()` and `flush()` raise `NotImplementedError`. The optional `[kafka]`
dependency is reserved for implementation. Credentials from `.env.example` are
not loaded yet. Future work should include a shared seeded scheduler, bounded
streaming batches, producer backpressure, delivery callbacks, retries, explicit
topic provisioning, graceful flush and rate/latency/drop metrics. Kafka
idempotence is a producer setting, not an end-to-end exactly-once guarantee.

Docker Compose is an untested single-node KRaft development placeholder, with a
`kafka` profile and host-local port binding. Set `KAFKA_IMAGE_TAG` to a tested
pinned Apache Kafka release before `docker compose --profile kafka up -d`.
The required variable can also be needed for Compose config parsing even when
the profile is disabled. No broker is started by Python or validation tests.
TLS/SASL, persistence, resilience and production operations are future work.

Contract tests run now. Explicitly skipped simulation/transport tests mark later
acceptance work; their skips must not be interpreted as successful load tests.
