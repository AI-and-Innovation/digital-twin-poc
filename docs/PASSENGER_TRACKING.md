# Terminal simulation and passenger tracking - RGIA Terminal 1

One simulation of Hyderabad's Terminal 1 produces four feeds. Every number comes from the same synthetic
passengers, so queue counts, zone counts and passenger dots always agree. **All passengers are synthetic.**

```
Flights -> passengers -> walk, queue, dwell, board / reclaim, exit        (generators/passenger.py)
                 |
                 +--> passenger.position     every 5 s     one record per passenger
                 +--> passenger.touchpoint   every minute  queue, waits, open lanes, target status per checkpoint
                 +--> passenger.aggregate    every minute  people and density per zone
                 +--> passenger.journey_kpi  every minute  kerb-to-airside, arrival-to-exit, headcounts
                 |
        Kafka topics (airport-eye.poc.esri.<feed>) or files (CSV / GeoJSON)
                 |
        ArcGIS Velocity + Dashboards   /   the local dashboard (scripts/map_viewer.py)
```

## 1. Quick start

```bash
scripts/local_poc.sh setup           # once
scripts/local_poc.sh map             # dashboard with the simulation built in: no Kafka, no Docker
scripts/local_poc.sh demo            # Kafka + all four feeds + dashboard (needs Docker)
scripts/local_poc.sh pax-files --minutes 15   # all four feeds for 15 minutes as files
scripts/local_poc.sh pax-files --live         # rewrite the *_latest files continuously
```

Open <http://127.0.0.1:8080>. Add `--speed 12` to `map` or `demo` to run 12 times faster (use real time,
the default, when Esri is ingesting). Add `--scenario peak` for a busy terminal at any hour: the morning-peak
load all day with a few lanes fewer open, so queues and alerts show up in a demo (scenarios live in
`config/passenger_tracking.json` under `scenarios`). The feed is paced by the wall clock: if the laptop
sleeps, it catches up on wake instead of running behind. The first records appear a few seconds after start: the terminal is
simulated silently from local midnight first, so "served since midnight" is complete.

## 2. The dashboard

What an operations room would look at, modelled on Esri airport dashboards and passenger-flow products
(Xovis, Veovo):

- **Top tiles:** people in the terminal, people queuing, median kerb-to-airside time, median
  arrival-to-exit time, the longest estimated wait and the number of alerts.
- **Map, one floor at a time** (L1 departures, L0 arrivals): the real terminal outline, zones coloured by
  density, checkpoints coloured by status (on target, busy, over target) and sized by queue, passengers
  as dots coloured by activity, and an optional heatmap.
- **Checkpoints tab:** alerts (checkpoints over or near their wait target, crowded zones), then every
  checkpoint with queue, estimated wait and open lanes. Click one to fly to it; its popup has the
  60-minute queue chart.
- **Zones tab:** people per zone and density per 100 m², busiest first, with what they are doing.
- **Passenger tab:** click on or next to a dot to follow that passenger: journey, flight, gate, where they
  are now, their path on the map and a timeline of their journey.
- **Replay:** the slider at the bottom steps back through the last 90 minutes, one minute at a time.
- **Checkpoints up close:** zoom in (2D or 3D) and each checkpoint is drawn as it is: one block per open
  counter or lane along its service line, the queue area in front of it (grows with the queue, coloured
  by status) and a label with people waiting, wait time and lanes open.
- **Passengers:** move smoothly between the 5-second updates; in 3D, zoomed in, they are standing figures.
  Colour them by **activity**, **journey**, or **time to flight**: red = departing within 40 minutes and
  still landside (at risk of missing the flight), amber = within 40 minutes airside but not yet at the gate.
- **How it works tab:** the data flow and sources, in plain language.
- **3D view** (`3D view` button, or <http://127.0.0.1:8080/3d.html>): the terminal shell with both floors
  stacked (spacing adjustable), passengers at their floor height, a bar per checkpoint whose height is the
  queue, zone density as floor colour or height, and follow-a-passenger with a 3D trail. Tilt and rotate with
  right-drag or Ctrl-drag. Built with MapLibre GL and deck.gl, loaded from the jsDelivr CDN, so it needs
  internet (the 2D dashboard only needs it for the base map).

## 3. Where the coordinates come from

| Part | Source | How good |
|---|---|---|
| Terminal outline: west wing, central building, east wing | OpenStreetMap relation 13609059 "RGIA Terminal 1" | Real footprint, as mapped by OSM contributors |
| International Departures and General Aviation buildings | OpenStreetMap ways 1016464110 and 1016464107 | Real footprint |
| Bus gates 101-124 (east wing) and 131-137 (west wing) | OpenStreetMap `aeroway=gate` nodes | Real positions, pulled 10 m inside the building |
| Floors, zones, checkpoint positions, walkways, dwell areas | `scripts/build_terminal_layout.py` | **Schematic**: drawn inside the real outline, not the floor plan |

The layout is `config/terminal_layout.json`, in terminal-local metres (x east, y north) around 17.2355 N,
78.4295 E; `spatial.LocalFrame` converts to WGS84 (a few centimetres of error over the terminal).

Assumptions in the schematic, to confirm against GHIAL drawings: departures upstairs (L1), arrivals and
reclaim on the ground (L0); domestic processing in the central building and east wing; international
check-in, security and emigration in the International Departures block and west wing; bus gates for
departures and bus arrivals at ground level.

**To use the real floor plan:** get GHIAL's CAD/BIM (or the Esri team's ArcGIS Indoors model), redraw the
zones, checkpoints and walkways from it (in the builder, or write `terminal_layout.json` directly), and
set `frame` from two or three surveyed control points if the drawings have their own coordinates.
Nothing else changes. To refresh the OSM part: `python scripts/build_terminal_layout.py`
(or `--osm-json config/osm_rgia_t1.json` to rebuild from the saved extract).

OpenStreetMap data is (c) OpenStreetMap contributors, available under the ODbL. Keep the attribution
wherever the outlines are shown.

## 4. How the simulation works (`src/airport_eye/generators/passenger.py`, `config/passenger_tracking.json`)

1. **Flights** per journey type, shaped by an hourly profile (busy 06:00-09:00 and 17:00-20:00, quiet
   at night). Each departure gets a gate on its pier.
2. **Passengers.** Departures reach the kerb 60-120 min (domestic) or 120-200 min (international) before
   departure. Arrivals come off buses, 60 per bus, every 3 minutes after landing.
3. **Journeys:**
   - Domestic departure: entry (manual, DigiYatra or priority) → check-in (counter, kiosk then bag drop,
     or none with web check-in; 3% use city-side check-in) → security → shops and seating → gate → boarding.
   - International departure: entry → check-in → security → emigration (manual, e-gate or priority) →
     west wing → gate → boarding.
   - International arrival: immigration (counter or e-gate) → reclaim → arrivals hall → exit.
   - Domestic arrival: reclaim → arrivals hall → exit. General aviation runs through the GA building.
4. **Walking** follows the walkway graph at 0.9-1.5 m/s. Landside, airside and arrivals are separate
   walkway groups; checkpoints are the only crossings.
5. **Queues** are first-come-first-served lines at each checkpoint, served event by event. `open_channels`
   sets how many lanes or counters are open now (out of the installed number in the inventory); service
   times come from the inventory. Each gate has its own boarding queue.
6. **Reproducible:** the same seed and start time always give the same passengers.

## 5. The four feeds

All four are flat JSON (the Esri record), keyed by `entity_id`, with `event_time_epoch_ms` as the time.

**`passenger.position`** (topic `airport-eye.poc.esri.passenger.position`, every 5 s, one per passenger)

| Field | Meaning |
|---|---|
| `passenger_id` (= `entity_id`), `pnr_id`, `flight_id`, `gate_id` | Synthetic IDs |
| `journey` | `domestic_departure`, `international_departure`, `domestic_arrival`, `international_arrival`, `ga_departure`, `ga_arrival` |
| `activity` | `walking`, `queuing`, `in_service`, `dwelling`, `at_gate`, `reclaim`; the last record of a track is `boarded` or `exited` |
| `touchpoint_id` | Checkpoint being queued at or served by |
| `longitude`, `latitude`, `floor`, `elevation_m` | Position (WGS84), floor (`L1`, `L0`) and the floor's height above ground (0 or 8 m) |
| `indoor_x`, `indoor_y`, `indoor_z`, `indoor_zone_id`, `indoor_grid_id` | Terminal-local metres, zone, 5 m grid cell |
| `speed_mps`, `heading_deg`, `movement_state` | Over the last 5 s |
| `minutes_to_departure` | Departing passengers: minutes until their flight's scheduled departure (null for arrivals) |

**`passenger.touchpoint`** (every minute, 16 checkpoints: entry, check-in, security, boarding, immigration)
- the same fields as the queue feed in `docs/ESRI_SETUP.md`, now measured from the passengers.
`service_channels` is the number of lanes or counters **open**; boarding sums every gate of the pier.

**`passenger.aggregate`** (every minute, one per zone)

| Field | Meaning |
|---|---|
| `zone_id` (= `entity_id`), `zone_name`, `floor` | Zone (same IDs as `indoor_zone_id`) |
| `passenger_count`, `area_m2`, `density_per_100m2` | People in the zone at the end of the minute |
| `people_walking`, `people_queuing`, `people_in_service`, `people_dwelling`, `people_at_gate`, `people_reclaim` | What they are doing |
| `longitude`, `latitude` | Zone centroid; `zone_occupancy_latest.geojson` has the zone polygons |
| `cells` | JSON string of 5 m cells with counts, for heatmaps |

**`passenger.journey_kpi`** (every minute, one record, `entity_id` = `HYD-T1-JOURNEYS`)

| Field | Meaning |
|---|---|
| `people_in_terminal`, `people_departing`, `people_arriving`, `people_queuing` | Headcounts now |
| `kerb_to_airside_p50_minutes`, `..._p90_minutes` | Kerb arrival to clearing security (domestic) or emigration (international), last 15 min |
| `airside_to_boarding_p50_minutes` | Time airside before boarding, last 15 min |
| `arrival_to_exit_p50_minutes`, `..._p90_minutes` | Off the bus to leaving the terminal, last 15 min |
| `longest_wait_touchpoint_id`, `longest_estimated_wait_seconds`, `touchpoints_busy`, `touchpoints_congested` | Checkpoint summary |

**Files** (`pax-files`): `passenger_positions_*.csv/.jsonl/.geojson`, `passenger_queues_*.csv/.geojson`,
`zone_occupancy_*.csv` plus `zone_occupancy_latest.geojson` (zone **polygons** with the counts),
`journey_kpis_*.csv/.json`. With `--live --history`, the minute feeds also append to `*_history.csv`.

## 6. How this maps to ArcGIS

| Feed | Suggested ArcGIS use |
|---|---|
| Positions | Velocity feed (Kafka, JSON), track ID `entity_id`, time `event_time_epoch_ms`, X/Y `longitude`/`latitude`; filter by `floor`. Heatmap renderer, and real-time geofencing against the zone polygons for enter/exit and dwell. |
| Zone occupancy | Join to the zone polygon layer (`zone_occupancy_latest.geojson`) for a density choropleth per floor. |
| Checkpoint queues | Point layer with status colours; ArcGIS Dashboards list and gauges for wait against target; alert when `status` is `congested`. |
| Journey KPIs | Dashboards indicators and line charts. |

**3D in ArcGIS:** every located record (positions, checkpoints, zones) carries `elevation_m`, the floor's
height above the ground floor. Use it as Z in a scene layer (ArcGIS Scene Viewer, or ArcGIS Indoors 3D on
the real floor plans) with "relative to ground" placement. Positions also keep `indoor_z`, the same value.

## 7. Limits

- The interior is schematic: zones, counters and walkways are plausible, not surveyed.
- Rates, shares, open lanes and timings are assumptions, not RGIA data.
- Positions are exact (simulation truth). Real positions would come from Wi-Fi/BLE or camera analytics
  with metres of error; checkpoint counts from 3D sensors. With real people, IDs must be anonymised
  (DPDP Act 2023).
- Floors are simple: departures stay on L1, arrivals on L0. No escalators or lifts are modelled.
- The older standalone queue feed (`scripts/local_poc.sh stream` / `files`) still exists for the
  queue-only hand-off; it is not linked to passengers. Don't run it and `demo` at the same time: both
  publish to the touchpoint topic.
