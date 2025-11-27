\# VLC Pipeline Monitoring



\## Overview

Prometheus-based monitoring for Kafka, Connect, and data ingestion health.



\## Components

\- \*\*Prometheus\*\*: Scrapes JMX exporters on port 9404

\- \*\*Grafana\*\*: Dashboards at `/grafana/` (via reverse proxy)



\## Scraped Targets

| Service         | Endpoint             | Metrics                           	|

|-----------------|----------------------|--------------------------------------|

| Kafka           | kafka:9404           | Broker state, partitions, throughput |

| Connect         | connect:9404         | Connector/task status, failures   	|

| Schema Registry | schema-registry:9404 | Registry health                   	|



\## Alerts

\- \*\*KafkaOfflinePartitions\*\*: Critical if any partition is offline

\- \*\*ConnectTaskFailed\*\*: Critical if JDBC sink tasks fail

\- \*\*NoAirDataIngested / NoWeatherDataIngested\*\*: Warning if no data for 30min



\## Adding Prometheus to docker-compose.yml

Prometheus has been added to the compose file.



