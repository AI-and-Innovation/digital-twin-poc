"""Derive the Delhi (DIAL, IGI Terminal 3) configs in config/del/ from the Hyderabad ones.

Same simulation, same record format; what changes for Delhi:
  * ids: SYN-HYD-* -> SYN-DEL-*, terminal DEL-T3, airport DEL
  * T3 has no general aviation terminal and no city-side check-in: those touchpoints go
  * busier than RGIA T1 (assumed rates, not DIAL data): more flights, more lanes open,
    more ground handling vehicles
  * Kafka topics: aiop-del-topic-* (kept apart from the Hyderabad aiop-hyd-topic-* ones)

  python scripts/del/make_configs.py
"""

from __future__ import annotations

import copy
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC, OUT = ROOT / "config", ROOT / "config" / "del"
DROP = ("-GA-", "-CITY-")

# Sized from the tuned Hyderabad values, scaled by demand: Delhi T3 is assumed to carry
# ~1.7x Hyderabad's domestic and ~3x its international passengers (see FLIGHTS).
# touchpoint -> service channels installed at Delhi T3 (assumed)
CHANNELS = {"ENTRY": 16, "DIGIYATRA-ENTRY": 8, "PRM-ENTRY": 2, "CHECKIN-DOM": 34, "CHECKIN-INT": 40,
            "KIOSK-DOM": 14, "KIOSK-INT": 13, "BAGDROP-DOM": 17, "BAGDROP-INT": 16, "SECURITY-DOM": 34,
            "SECURITY-INT": 30, "SECURITY-PRM": 2, "EMIGRATION": 46, "EMIGRATION-EGATE": 9, "EMIGRATION-PRM": 3,
            "BOARDING-DOM": 20, "BOARDING-INT": 18, "IMMIGRATION": 46, "IMMIGRATION-EGATE": 9,
            "RECLAIM-DOM": 170, "RECLAIM-INT": 160}
# lanes open now (out of the above); the peak scenario opens a few fewer
OPEN = {"ENTRY": 7, "DIGIYATRA-ENTRY": 2, "CHECKIN-DOM": 18, "CHECKIN-INT": 25, "SECURITY-DOM": 19,
        "SECURITY-INT": 15, "EMIGRATION": 17, "IMMIGRATION": 28}
PEAK_OPEN = {"ENTRY": 8, "CHECKIN-DOM": 21, "CHECKIN-INT": 28, "SECURITY-DOM": 22, "SECURITY-INT": 18,
             "EMIGRATION": 19, "IMMIGRATION": 30}
FLIGHTS = {"domestic_departure": {"per_hour": 9, "passengers": [100, 170], "arrive_before_departure_minutes": [60, 120]},
           "international_departure": {"per_hour": 3, "passengers": [180, 300],
                                       "arrive_before_departure_minutes": [120, 200]},
           "domestic_arrival": {"per_hour": 9, "passengers": [100, 170]},
           "international_arrival": {"per_hour": 3, "passengers": [180, 300]},
           "ga_departure": {"per_hour": 0, "passengers": [2, 6], "arrive_before_departure_minutes": [30, 60]},
           "ga_arrival": {"per_hour": 0, "passengers": [2, 6]}}
GSE_FLEET = {"baggage_tractor": 60, "fuel_truck": 16, "catering_truck": 16, "apron_bus": 30, "pushback_tug": 14,
             "gpu": 52}
TOPICS = {"passenger.position": "aiop-del-topic-pax-position-event-silver-v1",
          "passenger.touchpoint": "aiop-del-topic-pax-queue-event-gold-v1",
          "passenger.aggregate": "aiop-del-topic-pax-zone-event-gold-v1",
          "passenger.journey_kpi": "aiop-del-topic-pax-journey-kpi-event-gold-v1",
          "vehicle.position": "aiop-del-topic-veh-position-event-silver-v1",
          "curb.occupancy": "aiop-del-topic-curb-occupancy-event-gold-v1",
          "road.traffic": "aiop-del-topic-road-traffic-event-gold-v1",
          "gse.position": "aiop-del-topic-gse-position-event-silver-v1",
          "stand.activity": "aiop-del-topic-stand-activity-event-gold-v1"}


def rename(value):
    """SYN-HYD-* -> SYN-DEL-*, everywhere in a JSON value."""
    return json.loads(json.dumps(value).replace("SYN-HYD-", "SYN-DEL-").replace("HYD-MAIN", "DEL-T3"))


def dropped(tp_id: str) -> bool:
    return any(d in tp_id for d in DROP)


def load(name: str):
    return json.loads((SRC / name).read_text(encoding="utf-8"))


def save(name: str, value) -> None:
    (OUT / name).write_text(json.dumps(value, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote config/del/{name}")


def main() -> None:
    OUT.mkdir(exist_ok=True)

    inv = rename(load("hyderabad_touchpoints.json"))
    inv.update(airport_id="DEL", airport_name="Indira Gandhi International Airport, Delhi (DIAL) - Terminal 3",
               scope=("Passenger processing and assistance touchpoints at Terminal 3: departures, arrivals and "
                      "transfers; excludes retail, dining and landside transport."))
    inv["touchpoints"] = [t for t in inv["touchpoints"] if not dropped(t["touchpoint_id"])]
    for t in inv["touchpoints"]:
        t["terminal_id"] = "DEL-T3"
        name = t["touchpoint_id"].removeprefix("SYN-DEL-")
        if name in CHANNELS:
            t["simulation"]["service_channels"] = CHANNELS[name]
    save("delhi_touchpoints.json", inv)

    targets = rename(load("service_targets.json"))
    targets["touchpoints"] = {k: v for k, v in targets["touchpoints"].items() if not dropped(k)}
    save("service_targets.json", targets)

    cfg = rename(load("passenger_tracking.json"))
    cfg["_note"] = ("Synthetic passenger tracking for Delhi IGI (DIAL) Terminal 3, derived from the Hyderabad "
                    "config by scripts/del/make_configs.py. Flight rates, passenger counts, shares, lanes open and "
                    "fleet sizes are ASSUMPTIONS, not DIAL data.")
    cfg.update(id_prefix="SYN-DEL", terminal_id="DEL-T3", kpi_id="DEL-T3-JOURNEYS",
               layout_file="terminal_layout.json", inventory_file="delhi_touchpoints.json",
               service_targets_file="service_targets.json", ground_layout_file="ground_layout.json",
               output_directory="../../generated_data/del", flights=FLIGHTS)
    cfg["shares"]["domestic_city_checkin"] = 0.0
    cfg["open_channels"] = {f"SYN-DEL-{k}": v for k, v in OPEN.items()}
    peak = cfg["scenarios"]["peak"]
    peak["open_channels"] = {f"SYN-DEL-{k}": v for k, v in PEAK_OPEN.items()}
    cfg["ground"]["gse_fleet"] = GSE_FLEET
    cfg["ground"]["car_park_initial_share"] = 0.55
    save("passenger_tracking.json", cfg)

    proxy = copy.deepcopy(load("rest_proxy.json"))
    proxy["_note"] = proxy["_note"].replace("terminal simulation", "Delhi T3 terminal simulation")
    proxy["topics"] = TOPICS
    save("rest_proxy.json", proxy)


if __name__ == "__main__":
    main()
