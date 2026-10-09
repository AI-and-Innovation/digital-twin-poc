# Passenger tracking - RGIA Terminal 1

Synthetic passengers moving through Hyderabad's Terminal 1, published every 5 seconds with
latitude/longitude, floor, zone and what each person is doing. **All passengers are synthetic.**

## 1. Where the coordinates come from

| Part | Source | How good |
|---|---|---|
| Terminal outline: west wing, central building, east wing | OpenStreetMap relation 13609059 "RGIA Terminal 1" | Real footprint, as mapped by OSM contributors |
| International Departures and General Aviation buildings | OpenStreetMap ways 1016464110 and 1016464107 | Real footprint |
| Bus gates 101-124 (east wing) and 131-137 (west wing) | OpenStreetMap `aeroway=gate` nodes | Real positions, pulled 10 m inside the building |
| Floors, zones, touchpoint positions, walkways, dwell areas | `scripts/build_terminal_layout.py` | **Schematic**: drawn inside the real outline, not the floor plan |

The layout lives in `config/terminal_layout.json`, in **terminal-local metres** (x east, y north) around
the origin 17.2355 N, 78.4295 E. `spatial.LocalFrame` converts metres to WGS84. Over a 1 km terminal the
conversion error is a few centimetres; the schematic interior is the real limit.

Assumptions in the schematic (to confirm against GHIAL drawings):

- Departures on the upper level (L1), arrivals and baggage reclaim on the ground level (L0).
- Domestic processing in the central building and the east wing; international check-in, security and
  emigration in the International Departures block and the west wing.
- Entry, check-in and security run from the kerb (north) towards the apron (south).
- Departing passengers use the bus gates; arriving passengers come in by bus at ground level.

**To use the real floor plan:** get the CAD/BIM drawings from GHIAL (or the Esri team's ArcGIS Indoors
model), then replace the zone, touchpoint and walkway coordinates in the builder, or write
`terminal_layout.json` from the drawings directly. If the drawings use their own coordinate system,
set `frame` from two or three surveyed control points. The simulator and the feed don't change.

To refresh the OSM part: `python scripts/build_terminal_layout.py` (fetches from Overpass), or
`--osm-json config/osm_rgia_t1.json` to rebuild from the saved extract.

OpenStreetMap data is (c) OpenStreetMap contributors, available under the ODbL. Keep the attribution
wherever the outlines are shown.

## 2. How the simulation works (`src/airport_eye/generators/passenger.py`)

1. **Flights** are generated per journey type (`config/passenger_tracking.json`): domestic and
   international departures and arrivals, plus general aviation. Each departure gets a gate from its
   pier (a gate is reused only after 50 minutes).
2. **Passengers.** Departing passengers reach the kerb 60-120 minutes (domestic) or 120-200 minutes
   (international) before departure. Arriving passengers come off buses, 60 to a bus, every 3 minutes
   after landing.
3. **Journey plans:**
   - Domestic departure: entry (manual, DigiYatra or priority) → check-in (counter, or kiosk then
     bag drop, or skip with web check-in) → security → shops and seating → gate → boarding.
     About 3% use the city-side check-in first.
   - International departure: entry → check-in (International Departures block) → security →
     emigration (manual, e-gate or priority) → west wing → gate → boarding.
   - International arrival: immigration (counter or e-gate) → baggage reclaim → arrivals hall → exit.
   - Domestic arrival: baggage reclaim → arrivals hall → exit.
4. **Walking** follows the walkway graph at 0.9-1.5 m/s. Landside, airside and arrivals are separate
   walkway groups; the touchpoints are the only crossings, so nobody walks through security.
5. **Queues** are first-come-first-served lines at each touchpoint's position, with the counter or lane
   count and service times from `config/hyderabad_touchpoints.json`. People queue in a serpentine in
   front of the service line, so long queues are visible on the map. Each gate has its own boarding
   queue (2 lanes, 5-12 s per person).
6. **Warm-up.** The terminal is simulated for 4 hours before the first record, so it starts full.
7. **Reproducible.** The same seed and start time always give the same passengers and tracks.

Default volume is laptop-sized: about 1,200-2,200 people in the terminal at once, about 400 position
records per second. Raise `per_hour` and `passengers` in the config for load tests.

## 3. What is published

One record per passenger every 5 seconds, in the same flat Esri format as the queue feed.

| | |
|---|---|
| Kafka topic | `airport-eye.poc.esri.passenger.position` |
| Message key | `HYD:<passenger_id>` |
| Track ID | `entity_id` (= `passenger_id`) |
| Time | `event_time_epoch_ms` |
| Geometry | `longitude`, `latitude` (WGS84) plus `floor` |

| Field | Meaning |
|---|---|
| `passenger_id`, `pnr_id`, `flight_id` | Synthetic IDs (`SYN-...`) |
| `journey` | `domestic_departure`, `international_departure`, `domestic_arrival`, `international_arrival`, `ga_departure`, `ga_arrival` |
| `activity` | `walking`, `queuing`, `in_service`, `dwelling`, `at_gate`, `reclaim`; the **last** record of a track is `boarded` or `exited` |
| `touchpoint_id` | Touchpoint being queued at or served by (same IDs as the queue feed); boarding uses `SYN-HYD-BOARDING-DOM/INT` |
| `gate_id` | Departure gate (`SYN-HYD-GATE-101` ...) |
| `longitude`, `latitude`, `floor` | Position (WGS84) and floor (`L1` departures, `L0` arrivals) |
| `indoor_x`, `indoor_y`, `indoor_z` | Position in terminal-local metres (`indoor_coordinate_frame_id` = `HYD-T1-LOCAL-METRES`) |
| `indoor_zone_id` | Zone the passenger is in (same zone IDs as the queue feed) |
| `indoor_grid_id` | 5 m grid cell, `<floor>-<ix>-<iy>`, for heatmaps |
| `speed_mps`, `heading_deg`, `movement_state` | Speed over the last 5 s, compass heading, `walking` or `stationary` |

Esri feed settings: track ID `entity_id`, time `event_time_epoch_ms`, X `longitude`, Y `latitude`, WGS84.
Use `floor` to filter by level. A track that stops without `boarded`/`exited` has simply not been
updated yet.

## 4. Run it

```bash
scripts/local_poc.sh demo                                   # Kafka + queue feed + passenger tracking + map
scripts/local_poc.sh pax                                    # passenger tracking only, to Kafka
scripts/local_poc.sh pax-files --minutes 15                 # 15 minutes of positions to output/*.csv and *.jsonl
scripts/local_poc.sh pax-files --live                       # rewrite output/passenger_positions_latest.* every 5 s
scripts/local_poc.sh pax-files --start 2026-10-09T08:00:00+05:30 --minutes 60
```

On the map (<http://127.0.0.1:8080>) the dots are passengers, coloured by activity; click one for its
journey, flight, gate and zone. Use the floor buttons to show the departures or arrivals level. In the
Kafka UI the topic is `airport-eye.poc.esri.passenger.position`.

`--speed N` compresses time for a quick look (the demo uses `PAX_SPEED`, default 1). Leave it at 1 when
Esri is ingesting.

## 5. Limits

- The interior is schematic: zones, counters and walkways are plausible, not surveyed.
- Rates, shares and timings are assumptions, not RGIA data. The queue feed (`passenger.touchpoint`) still
  runs its own simulation; it is not yet derived from these passengers.
- Floors are kept simple: departing passengers stay on L1 (only the city-side check-in has a link up
  from L0) and arriving passengers stay on L0 from the bus onwards. No escalators or lifts are modelled.
- Real tracking would come from Wi-Fi/BLE or camera analytics. DigiYatra and boarding-pass scans only give
  checkpoint events. With real people, IDs must be anonymised (DPDP Act 2023).
