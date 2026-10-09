#!/usr/bin/env bash
# One-stop helper for running the POC on your own machine (macOS/Linux).
#   scripts/local_poc.sh setup        create .venv and install the project
#   scripts/local_poc.sh kafka-up     start a local Kafka in Docker and wait until healthy
#   scripts/local_poc.sh kafka-down   stop it (add -v to also delete its data)
#   scripts/local_poc.sh kafka-ui     browse topics and messages at http://127.0.0.1:8081
#   scripts/local_poc.sh test         unit tests + real-broker round trip
#   scripts/local_poc.sh demo         kafka + queue feed + passenger tracking + map viewer
#   scripts/local_poc.sh stream ...   just the publisher (extra args go to `airport-eye stream`)
#   scripts/local_poc.sh files ...    queue data as CSV/GeoJSON/JSON files, no Kafka (add --live for every minute)
#   scripts/local_poc.sh pax ...      live passenger tracking to Kafka (positions every 5 s)
#   scripts/local_poc.sh pax-files .. passenger tracking to CSV/JSONL files, no Kafka (add --live for latest files)
#   scripts/local_poc.sh viewer ...   just the map viewer
set -euo pipefail
cd "$(dirname "$0")/.."
BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
VENV=.venv

find_python() {
  for candidate in python3.13 python3.12 python3.11 python3; do
    if command -v "$candidate" >/dev/null 2>&1 && "$candidate" -c 'import sys; sys.exit(sys.version_info < (3, 11))' 2>/dev/null; then
      echo "$candidate"; return 0
    fi
  done
  echo "Python 3.11 or newer is required (macOS: brew install python@3.12)" >&2
  return 1
}

need_venv() {
  if [ ! -x "$VENV/bin/airport-eye" ]; then
    echo "Run 'scripts/local_poc.sh setup' first." >&2; exit 1
  fi
}

need_docker() {
  command -v docker >/dev/null 2>&1 || { echo "Docker is not installed or not on PATH." >&2; exit 1; }
  docker info >/dev/null 2>&1 || { echo "Docker is installed but not running. Start Docker Desktop and retry." >&2; exit 1; }
}

case "${1:-help}" in
  setup)
    PY="$(find_python)"
    [ -d "$VENV" ] || "$PY" -m venv "$VENV"
    "$VENV/bin/pip" install -q --upgrade pip
    "$VENV/bin/pip" install -q -e ".[kafka]"
    echo "Ready. Next: scripts/local_poc.sh files   (queue data as CSV/GeoJSON/JSON, no Kafka)  or  scripts/local_poc.sh demo   (live Kafka feed + map)"
    ;;
  kafka-up)
    need_docker
    docker compose --profile kafka up -d --wait kafka
    echo "Kafka is up on $BOOTSTRAP"
    ;;
  kafka-down)
    need_docker; shift
    docker compose --profile kafka --profile kafka-ui down "$@"
    ;;
  kafka-ui)
    need_docker
    docker compose --profile kafka --profile kafka-ui up -d --wait kafka kafka-ui
    echo "Kafka UI: http://127.0.0.1:8081"
    (command -v open >/dev/null 2>&1 && open "http://127.0.0.1:8081") || true
    ;;
  test)
    need_venv; need_docker
    "$VENV/bin/python" -m unittest discover -s tests
    "$VENV/bin/python" scripts/kafka_smoke_test.py --bootstrap "$BOOTSTRAP" --format esri --count 200
    ;;
  files)
    need_venv; shift
    "$VENV/bin/airport-eye" queue-files "$@"
    ;;
  pax)
    need_venv; shift
    "$VENV/bin/airport-eye" pax-stream --bootstrap "$BOOTSTRAP" "$@"
    ;;
  pax-files)
    need_venv; shift
    "$VENV/bin/airport-eye" pax-files "$@"
    ;;
  stream)
    need_venv; shift
    "$VENV/bin/airport-eye" stream --bootstrap "$BOOTSTRAP" "$@"
    ;;
  viewer)
    need_venv; shift
    "$VENV/bin/python" scripts/map_viewer.py --bootstrap "$BOOTSTRAP" "$@"
    ;;
  demo)
    need_venv; need_docker; shift
    docker compose --profile kafka up -d --wait kafka
    mkdir -p .poc
    "$VENV/bin/python" scripts/map_viewer.py --bootstrap "$BOOTSTRAP" > .poc/viewer.log 2>&1 &
    VIEWER_PID=$!
    # Passenger tracking runs alongside, in real time unless PAX_SPEED is set (log: .poc/pax.log).
    PYTHONUNBUFFERED=1 "$VENV/bin/airport-eye" pax-stream --bootstrap "$BOOTSTRAP" --speed "${PAX_SPEED:-1}" > .poc/pax.log 2>&1 &
    PAX_PID=$!
    trap 'kill "$VIEWER_PID" "$PAX_PID" 2>/dev/null || true' EXIT INT TERM
    sleep 2
    echo "Map: http://127.0.0.1:8080"
    (command -v open >/dev/null 2>&1 && open "http://127.0.0.1:8080") || (command -v xdg-open >/dev/null 2>&1 && xdg-open "http://127.0.0.1:8080") || true
    echo "Publishing one batch per minute. Add '--speed 60' for a quick look (one batch per second). Ctrl-C to stop."
    "$VENV/bin/airport-eye" stream --bootstrap "$BOOTSTRAP" "$@"
    ;;
  *)
    sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'
    ;;
esac
