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

## Where the data comes from

| Part | Source |
|---|---|
| Terminal 3 outline: processor, international pier (west), domestic pier (east) | OpenStreetMap relation 8632270 (`config/del/osm_del_t3.json`) |
| 51 contact gates (refs 01-62) | OpenStreetMap `aeroway=gate` (gate "23" on the T2 apron is dropped) |
| Landside roads, departure drives, arrivals forecourt road, T3 car parks, airport bus station | OpenStreetMap (`config/del/osm_del_ground.json`) |
| Floors, zones, checkpoints, walkways inside the building | Schematic: `scripts/del/build_terminal_layout.py` |
| Stands and apron roads | Schematic: OSM maps almost none at T3; one stand 70 m out from each gate lounge, service roads along the piers (`scripts/del/build_ground_layout.py`) |
| Flights, passengers, lanes open, fleets | Assumed: `scripts/del/make_configs.py` (Delhi sized from the tuned Hyderabad values: ~1.7x domestic, ~3x international) |

The local frame is rotated 60 degrees so the building's sides run along the axes (`+y` landside). Records
still carry plain WGS84 longitude/latitude and compass headings.

## Rebuild after changes

```bash
.venv/bin/python scripts/del/build_terminal_layout.py --osm-json config/del/osm_del_t3.json
.venv/bin/python scripts/del/build_ground_layout.py --osm-json config/del/osm_del_ground.json
.venv/bin/python scripts/del/make_configs.py
```

## Waiting on

- **GeoKno's selected POC area.** The data covers all of Terminal 3 for now. Once Parag confirms the area, the zones
  and checkpoints outside it can be dropped (and the position volume falls with them).
- **DIAL BIM floor levels.** Floors are placeholders: departures `L1` at 10 m, arrivals `L0` at 0 m, ground 237 m
  above sea level (`reference_elevation_m`). Replace them in `scripts/del/build_terminal_layout.py` (`FLOORS`,
  `REFERENCE_ELEVATION_M`) and rebuild.
- **Kafka:** the nine `aiop-del-topic-*` topics need creating, and Esri/GeoKno need read-only consumer access.
