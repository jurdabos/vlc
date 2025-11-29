# Grafana Setup Complete

## Refactoring Summary
- **Before**: Grafana configuration in `/opt/vlc/compose/grafana`
- **After**: Moved to repo root at `/opt/vlc/grafana`
- **Reason**: Better organization with grafana configs at root level

## Directory Structure
```
grafana/
├── README.md                          # Documentation
├── dashboards/
│   ├── air_quality.json              # Air quality dashboard with spatial queries
│   └── system_metrics.json           # Placeholder for system metrics
└── provisioning/
    ├── datasources/
    │   ├── timescaledb.yml           # PostgreSQL/TimescaleDB datasource
    │   └── prometheus.yml            # Prometheus datasource
    └── dashboards/
        └── dashboards.yml            # Dashboard provider config
```

## Status
✅ Grafana running on port 3000
✅ TimescaleDB datasource provisioned (timescaledb:5432, DB: air)
✅ Prometheus datasource provisioned (http://prometheus:9090)
✅ Air Quality dashboard provisioned with SQL queries:
   - Total row count
   - NO2 levels for Patraix (last 24h)
   - Latest 5 NO2 readings for Patraix
   - Spatial query: stations within 1km of coordinates
   - All stations NO2 levels (last 7 days)

## Access
- **URL**: http://localhost:3000/grafana/
- **Credentials**: admin/admin
- **Subpath**: /grafana/ (configured via GF_SERVER_SERVE_FROM_SUB_PATH)

## Next Steps
1. Run producers to populate TimescaleDB with data
2. Access Grafana and verify dashboards display data
3. Add more panels to system_metrics.json for Kafka/Connect monitoring

## Testing Queries
Once data is available, test these in Grafana Explore or psql:

```sql
-- Count arriving rows
SELECT count(*) FROM air.hyper;

-- Latest 5 readings for NO2 in Patraix
SELECT ts, nombre, no2
FROM air.hyper
WHERE nombre ILIKE 'Patraix'
ORDER BY ts DESC
LIMIT 5;

-- Spatial: stations within 1 km of 39.47N, -0.376E
SELECT nombre, direccion
FROM air.hyper
WHERE ST_DWithin(
  geom,
  ST_SetSRID(ST_MakePoint(-0.376, 39.470), 4326)::geography,
  1000
)
GROUP BY nombre, direccion
ORDER BY nombre;
```
