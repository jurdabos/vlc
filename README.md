\# vlc/README.md



This repository contains the infrastructural elements of a pipeline related to a study project for data engineering at IU.



\## Data Flow



```

Valencia ODS API (v2.1)

&nbsp;   ↓ (poll every 5min)

Producer (air/weather)

&nbsp;   ↓ (fingerprint dedup)

Kafka (vlc.air / vlc.weather)

&nbsp;   ↓ (JDBC Sink Connector)

TimescaleDB (air/weather schemas)

&nbsp;   ↓ (queries)

Grafana Dashboards

```

