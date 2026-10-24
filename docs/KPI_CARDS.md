# AE-IT widget cards (KPI feeds), Hyderabad

The simulation computes the finished values of the AE-IT dashboard cards every minute and publishes them as four
more feeds, next to the passenger and vehicle feeds (`src/airport_eye/generators/kpi.py`, switched on by the
`kpi` section of `config/passenger_tracking.json`). **All data is synthetic.**

| Event type | Kafka topic (REST proxy) | One record | Track ID |
|---|---|---|---|
| `kpi.checkpoint` | `aiop-hyd-topic-kpi-checkpoint-event-gold-v1` | per desk / lane / counter per minute | `checkpoint_id` |
| `kpi.curbside` | `aiop-hyd-topic-kpi-curbside-event-gold-v1` | departures and arrivals curbside per minute | `curbside_id` |
| `kpi.overview` | `aiop-hyd-topic-kpi-overview-event-gold-v1` | airport and terminal overview per minute | `overview_id` |
| `flight.status` | `aiop-hyd-topic-flight-event-silver-v1` | per flight when it is first known and at each milestone | `flight_id` |

Every record is flat JSON like the other feeds: `entity_id` (= the track ID above), `event_time_epoch_ms`,
`longitude` / `latitude` (the desk, the kerb, the terminal, or the stand). The 24-hour trend fields
(`today_hourly_*`) are JSON arrays sent as strings, one value per local hour (null for hours still to come).

## Cards and fields

**Desk cards** (`kpi.checkpoint`): domestic check-in desks 01, 05, 09; domestic security lanes 01, 06;
emigration counters 02, 05 (chosen in the config).

| Field | Meaning |
|---|---|
| `queue_length` | Waiting in front of the desk now: its share of the area's shared queue |
| `avg_processing_time_seconds` | Mean service time of passengers this desk finished in the last 15 min |
| `avg_wait_time_seconds` | Mean time from joining the queue to reaching this desk, services started in the last 15 min |
| `happiness_index` | Happy < 5 min, Neutral 5-10 min, Unhappy > 10 min wait (until "FRT" is defined) |
| `hour_avg_wait_minutes`, `previous_hour_avg_wait_minutes`, `today_hourly_avg_wait_minutes` | Wait trend; threshold `wait_threshold_minutes` (10) |

**Curbside cards** (`kpi.curbside`): people within 12 m of the kerb, live vehicles at the bays, vehicles this
clock hour and % vs the previous hour (to the same minute), average dwell this hour and % vs the previous hour,
`this_hour_<type>` (private car, app cab, taxi, bus), `today_hourly_vehicles`, and on the departures card
`trolleys_available` / `trolleys_total` (an assumed model: the simulation has no trolleys).

**Overview cards** (`kpi.overview`, for the clock hour so far; the record at :00 closes the hour):
arrival / departure flights and % vs scheduled, stand occupancy, OTA / OTD (within +/- 3 min; ATA = in-block,
ATD = off-block), average delays, PAX forecast vs actual (`today_hourly_*` for the graph). The airport card adds
taxi-in / taxi-out vs target (8 / 12 min); the terminal card adds arrival / departure bags vs threshold (the bags
expected from the flights scheduled that hour).

**Flights** (`flight.status`): `status` (scheduled, landed, on_stand / scheduled, departed, airborne),
`scheduled_time`, `estimated_time`, `landing_time` (ALDT), `in_block_time` (AIBT), `off_block_time` (AOBT),
`takeoff_time` (ATOT), `delay_minutes`, `on_time`, `taxi_minutes`, `stand_id`, `gate_id`, `pax_forecast`,
`pax_actual`, `bags`. The first minute of a run sends every known flight; after that, only changes.

## Real and assumed

- From the simulation: who is served at which desk and for how long, kerb stops and dwell, who stands at the kerb,
  each flight's in-block / off-block time, passengers and stand.
- Drawn per flight (repeatably, from the flight ID): schedule = actual minus a delay (62% within 3 min, a tail to
  75 min, a few early), estimate, taxi times (4-12 / 8-20 min), forecast passengers (-10% to +12%), bags (1.1 per
  domestic, 1.6 per international passenger with bags). Arrivals are generated 2 h ahead so their schedule is
  known before a late flight lands.
- Not included: bikes (no two-wheelers in the simulation), general aviation flights.

## Run it

```bash
.venv/bin/airport-eye pax-stream --rest-proxy --feeds all --interval 30      # live, all feeds incl. the KPI cards
.venv/bin/airport-eye pax-files --feeds kpis --minutes 60                     # one hour of the KPI feeds to output/
.venv/bin/python scripts/make_checkpoint_kpis.py                              # one day as an Excel workbook + CSVs
```
