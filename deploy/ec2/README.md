# Hyderabad publisher on EC2

This runs the same live feed that is running on the Mac: all 13 Hyderabad topics, positions every 30 seconds, through the Kafka REST proxy. It keeps going until the service is stopped.

The 13 topics are the 9 passenger and vehicle feeds plus the 4 AE-IT KPI feeds (`docs/KPI_CARDS.md`):
`aiop-hyd-topic-kpi-checkpoint-event-gold-v1`, `aiop-hyd-topic-kpi-curbside-event-gold-v1`,
`aiop-hyd-topic-kpi-overview-event-gold-v1`, `aiop-hyd-topic-flight-event-silver-v1`. If a KPI topic is missing,
the log says `KPI topics not created yet` and the other feeds keep going.

## Give him this folder

`digital-twin-poc-local-esri/` without `.venv`, `output`, or `.poc`.

He also needs `/etc/airport-eye.env` on the server. Send that password in a separate message. Do not put it in git or in this folder.

## Server

- Amazon Linux 2023 or Ubuntu 22.04, `t3.small` is enough.
- Outbound HTTPS only. No public inbound port is required for the feed. SSH from his IP, or Session Manager.
- Python 3.11.

Amazon Linux:

```sh
sudo dnf install -y python3.11 tar
```

Ubuntu:

```sh
sudo apt-get update && sudo apt-get install -y python3.11 python3.11-venv tar
```

## Env file

```sh
sudo cp deploy/ec2/airport-eye.env.example /etc/airport-eye.env
sudo chmod 600 /etc/airport-eye.env
sudo vi /etc/airport-eye.env
```

```
KAFKA_REST_USER=admin
KAFKA_REST_PASSWORD=<the proxy password>
KAFKA_REST_URL=https://dev-aop-kafka-rest-proxy.wdplatform.com
```

## Install and start

From the project root on the server:

```sh
sudo sh deploy/ec2/setup.sh
```

The service command is:

```sh
airport-eye pax-stream --rest-proxy --feeds all --interval 30 --speed 1
```

Logs:

```sh
journalctl -u airport-eye-pax.service -f
```

A healthy line looks like `batch 2: ... (delivered ..., failed 0)`. The first line can take a minute while the simulation warms up from local midnight. The clock in the data is Asia/Kolkata from `config/passenger_tracking.json`. The server itself can stay on UTC.

Stop or restart:

```sh
sudo systemctl stop airport-eye-pax.service
sudo systemctl restart airport-eye-pax.service
```

## Status page

`setup.sh` also starts a page on the server at `http://127.0.0.1:8787`. It shows On or Off, uptime, how many records were delivered, and the latest log lines. It is not reachable from the internet. From a laptop:

```sh
ssh -L 8787:127.0.0.1:8787 <user>@<ec2-host>
```

Then open `http://127.0.0.1:8787`. On this Mac, from the project root:

```sh
python3 deploy/ec2/publisher_status.py
```

## Updating a server that is already running

Copy the new folder to the server over the old one, then from the project root:

```sh
sudo sh deploy/ec2/setup.sh
sudo systemctl restart airport-eye-pax.service
```

`setup.sh` copies the new code to `/opt/airport-eye` and reinstalls it; the restart makes the running service pick
it up. `/etc/airport-eye.env` is kept. In the first log lines, `Topics ready:` should list all 13 topics.

## After it is up

Stop the copy on the Mac. Two publishers at once double the records.

In the Kafka UI, open `aiop-hyd-topic-pax-position-event-silver-v1` and confirm a new message about every 30 seconds,
and `aiop-hyd-topic-kpi-overview-event-gold-v1` for two new messages every minute.
