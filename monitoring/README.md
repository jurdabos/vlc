# VLC Pipeline Monitoring
Prometheus-based monitoring for Kafka, Connect, Schema Registry, and data ingestion health.

## Directory Structure
```
monitoring/
├── prometheus/
│   ├── prometheus.yml      # Scrape configs for all targets
│   └── alerts.yml          # Alert rules (Kafka, Connect, topic ingestion)
├── alertmanager/
│   └── alertmanager.yml    # Alert routing and email notifications
└── jmx-exporter/
    ├── jmx_prometheus_javaagent.jar
    ├── kafka.yml           # Kafka broker metrics
    ├── connect.yml         # Kafka Connect worker/task metrics
    └── schema-registry.yml # Schema Registry metrics
```

## Components
- **Prometheus** (port 9090): Scrapes JMX exporters every 15s
- **Alertmanager** (port 9093): Routes alerts to email
- **JMX Exporters**: Java agent attached to Kafka services, exposing metrics on ports 9404-9406
- **Grafana**: See `grafana/README.md` for dashboard configuration

## Scraped Targets
| Target | Port | Metrics |
|--------|------|--------|
| Kafka | 9404 | Broker state, partitions, throughput, request latency |
| Connect | 9406 | Connector/task status, batch sizes, record failures |
| Schema Registry | 9405 | Request counts, latencies, master election |
| Prometheus | 9090 | Self-monitoring |

## JMX Exporter Configs
### kafka.yml
- `kafka_broker_state` - Broker state (0=NotRunning, 3=RunningAsBroker)
- `kafka_under_replicated_partitions` - Under-replicated partition count
- `kafka_offline_partitions_count` - Offline partition count
- `kafka_topic_bytes_in_total` / `kafka_topic_bytes_out_total` - Per-topic throughput
- `kafka_topic_messages_in_total` - Per-topic message count
- `kafka_request_latency_ms_p99` - Produce/Fetch latency p99

### connect.yml
- `connect_worker_connector_count` / `connect_worker_task_count` - Worker metrics
- `connect_connector_status` - Connector status
- `connect_sink_task_record_read_total` / `connect_sink_task_record_send_total` - Records processed
- `connect_task_status` - Task running status
- `connect_task_record_failures_total` - Record failures

### schema-registry.yml
- `schema_registry_request_total` / `schema_registry_request_error_total` - Request counts
- `schema_registry_request_latency_avg` / `schema_registry_request_latency_max` - Latency
- `schema_registry_is_master` - Master election status

## Alert Rules
### Kafka Alerts
- **KafkaOfflinePartitions** (critical): Any partition offline for 1m
- **KafkaUnderReplicatedPartitions** (warning): Under-replicated partitions for 5m
- **KafkaBrokerDown** (critical): Broker unreachable for 1m

### Connect Alerts
- **ConnectTaskFailed** (critical): Task not running for 2m
- **ConnectRecordFailures** (warning): Record failures for 5m
- **ConnectDown** (critical): Connect unreachable for 1m

### Data Ingestion Alerts
- **NoAirDataIngested** (warning): No messages on `vlc.air` for 30m
- **NoWeatherDataIngested** (warning): No messages on `vlc.weather` for 30m

### Grafana-Managed Alerts
- **WeatherStationStale** (warning): a station's latest `weather.hyper` reading
  is older than 5h (one alert instance per `fiwareid`). Defined in
  `grafana/provisioning/alerting/vlc-staleness.yml`, evaluated by Grafana
  against TimescaleDB, and forwarded to Alertmanager
  (`handleGrafanaManagedAlerts: true`). Catches single dead stations the
  topic-level alerts above cannot see.

## Alertmanager Configuration
Alerts are grouped by `alertname` and `severity`, then routed to email. Critical alerts inhibit warning alerts for the same alertname.

SMTP configuration is provided via environment variables:
- `SMTP_HOST`, `SMTP_PORT`, `SMTP_FROM`, `SMTP_USER`, `SMTP_PASSWORD`

## Starting Monitoring Stack
```bash
docker compose -f compose/docker-compose.yml --profile ui up -d prometheus alertmanager grafana
```

## Verification
- Prometheus targets: http://localhost:9090/targets
- Alertmanager status: http://localhost:9093
- Grafana System Metrics dashboard: http://localhost:8080/grafana/
