# Delhi IGI (DIAL) Terminal 3

The same simulation and feeds as Hyderabad, for Delhi Terminal 3. It is kept apart from Hyderabad: its own files
in `config/del/`, IDs `SYN-DEL-*`, output in `output/del/`, local Kafka topics `airport-eye.poc.del.*` and
REST proxy topics `aiop-del-topic-*`. Nothing is published to the shared Kafka until those topics exist and
consumer access for Esri/GeoKno is agreed.

## Run it

Add `--airport del` to the usual commands:

```bash
.venv/bin/python scripts/map_viewer.py --simulate --airport del --port 8090     # dashboard: http://127.0.0.1:8090 (3D: /3d.html)
.venv/bin/airport-eye pax-files --airport del --minutes 15 --interval 30       # files to output/del/
.venv/bin/python scripts/export_map_layers.py --airport del --out output/del   # terminal and ground GeoJSON layers
.venv/bin/airport-eye pax-stream --airport del --rest-proxy --interval 30 --minutes 15   # only once the aiop-del topics exist
```

## Sample pack for Esri / GeoKno / DIAL

```bash
.venv/bin/python scripts/del/make_sample_pack.py     # share/delhi_sample/ and share/AirportEye_Delhi_T3_sample.zip
```

This runs the DIAL plan check, then 15 minutes of all nine feeds (CSV and JSONL), the map layers, a one-instant
snapshot GeoJSON, the plan fit drawing, a manifest with counts and SHA-256, a README with import steps, and the zip.
Use `--start`, `--minutes` and `--interval` for other windows.

## Where the data comes from

| Part | Source |
|---|---|
| Terminal 3 outline: processor, international pier (west), domestic pier (east) | OpenStreetMap relation 8632270 (`config/del/osm_del_t3.json`) |
| 51 contact gates (refs 01-62) | OpenStreetMap `aeroway=gate` (gate "23" on the T2 apron is dropped) |
| Landside roads, departure drives, arrivals forecourt road, T3 car parks, airport bus station | OpenStreetMap (`config/del/osm_del_ground.json`) |
| Departures level `L1`: checkpoints, walkways, gate waiting and dwell areas | DIAL floor plan (`config/del/dial_t3_departures_plan.geojson`, from GeoKno, 15 Oct 2026), fitted by `scripts/del/build_terminal_layout.py` |
| Arrivals level `L0`, floor heights, zone boundaries | Schematic: `scripts/del/build_terminal_layout.py` |
| Stands and apron roads | Schematic: OSM maps almost none at T3; one stand 70 m out from each gate lounge, service roads along the piers (`scripts/del/build_ground_layout.py`) |
| Flights, passengers, lanes open, fleets | Assumed: `scripts/del/make_configs.py` (Delhi sized from the tuned Hyderabad values: ~1.7x domestic, ~3x international) |

The local frame is rotated 60 degrees so the building's sides run along the axes (`+y` landside). Records
still carry plain WGS84 longitude/latitude and compass headings.

## The DIAL departures plan

`config/del/dial_t3_departures_plan.geojson` is Parag's file with the 3 `delete` and 3 uncategorised polygons dropped.
The builder puts the `L1` checkpoints on its counters and lanes. It then moves every `L1` walkway node, gate and checkpoint
onto open floor (`scripts/del/dial_plan.py`: walls, shops, toilets, offices, escalators and voids are closed; the unlabelled
`c2`-`c5` fixtures and queue barriers are not). Any walkway that cuts through something is routed round it on a 1 m grid.
International departures go through emigration, then security (`emigration_before_security` in
`config/del/passenger_tracking.json`).

Two assumptions to confirm with DIAL:
- the piers join the processor only across the large `stairs` areas at each pier head (ramps / escalators);
- the thin wall across the domestic pier head at local y ≈ -68 has a door (`ASSUMED_DOORS`).

Check after any change (it also runs in the tests):

```bash
.venv/bin/python scripts/del/check_dial_plan.py
```

## Rebuild after changes

```bash
.venv/bin/python scripts/del/build_terminal_layout.py --osm-json config/del/osm_del_t3.json
.venv/bin/python scripts/del/build_ground_layout.py --osm-json config/del/osm_del_ground.json
.venv/bin/python scripts/del/make_configs.py
```

## Waiting on

- **GeoKno's selected POC area.** The data covers all of Terminal 3 for now. Once Parag confirms the area, the zones
  and checkpoints outside it can be dropped (and the position volume falls with them).
- **DIAL arrivals floor plan** (`L0`) and a legend for the plan's `c2`/`c3`/`c4`/`c5` categories.
- **DIAL BIM floor levels.** Floors are placeholders: departures `L1` at 10 m, arrivals `L0` at 0 m, ground 237 m
  above sea level (`reference_elevation_m`). Replace them in `scripts/del/build_terminal_layout.py` (`FLOORS`,
  `REFERENCE_ELEVATION_M`) and rebuild.
- **Kafka:** the nine `aiop-del-topic-*` topics need creating, and Esri/GeoKno need read-only consumer access.
