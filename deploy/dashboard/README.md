# Live dashboard for the Hyderabad Kafka feed

This is the same map that was on http://127.0.0.1:8080. It reads the nine live `aiop-hyd-topic-*` topics and draws passengers, queues, zones, vehicles, kerbs and stands.

It uses the read-only Esri login, not the REST proxy admin password.

## Run

From the project root, with Python 3.11:

```sh
python3.11 -m venv .venv
.venv/bin/pip install -e '.[kafka]'
export KAFKA_SASL_USERNAME='ext-esri-reader'
export KAFKA_SASL_PASSWORD='<the Esri reader password, sent separately>'
.venv/bin/python scripts/map_viewer.py \
  --bootstrap dev-aop-kafka-ext.wdplatform.com:9097 \
  --security-protocol SASL_SSL \
  --sasl-mechanism SCRAM-SHA-512 \
  --rest-proxy config/rest_proxy.json \
  --group ext-del-esri-dashboard \
  --host 0.0.0.0 --port 8080
```

Open `http://<host>:8080`. The header should say `Live - kafka dev-aop-kafka-ext.wdplatform.com:9097`, not `in-process simulation`.

The consumer group must start with `ext-del-esri-`. Positions arrive every 30 seconds, so the dots update on that cadence.

In the cluster, run this as its own deployment and expose port 8080. Do not publish with this process. The publisher already running in namespace `airport-eye` is the writer.
