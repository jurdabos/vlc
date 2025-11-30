\# VLC Pipeline Monitoring


\## Overview

Prometheus-based monitoring for Kafka, Connect, and data ingestion health.


\## Components

\- \*\*Prometheus\*\*: Scrapes JMX exporters

\- \*\*Grafana\*\*: Dashboards at `/grafana/` (via reverse proxy)


\## Scraped Targets

Kafka for broker state, partitions and throughput

Connect for connector/task status and failures

Schema Registry for registry health to be included in ph3/4/5


\## Alerts

\- \*\*KafkaOfflinePartitions\*\*: Critical if any partition is offline

\- \*\*ConnectTaskFailed\*\*: Critical if JDBC sink tasks fail

\- \*\*NoAirDataIngested / NoWeatherDataIngested\*\*: Warning if no data for 30min




