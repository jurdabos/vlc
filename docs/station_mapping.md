# VLC Station Mapping Documentation
Updated: 2025-11-30

## Overview
This document maps historical station names from RVVCCA datasets to current FIWARE IDs from the Valencia Open Data API.

## Air Quality Stations (11 current)
| FIWARE ID | Current Name | Historical Name | Has Historical Data |
|-----------|--------------|-----------------|---------------------|
| A01_AVFRANCIA_60m | Francia | Avda. Francia | ✅ Yes |
| A02_BULEVARDSUD_60m | Boulevar Sur | Bulevard Sud | ✅ Yes |
| A03_MOLISOL_60m | Molí del Sol | Moli del Sol | ✅ Yes |
| A04_PISTASILLA_60m | Pista de Silla | Pista Silla | ✅ Yes |
| A05_POLITECNIC_60m | Universidad Politécnica | Politecnico | ✅ Yes |
| A06_VIVERS_60m | Viveros | Viveros | ✅ Yes |
| A07_VALENCIACENTRE_60m | Centro | Valencia Centro | ✅ Yes |
| A08_DR_LLUCH_60m | Dr. Lluch | - | ❌ New station |
| A09_CABANYAL_60m | Cabanyal | - | ❌ New station |
| A10_OLIVERETA_60m | Olivereta | Valencia Olivereta | ✅ Yes |
| A11_PATRAIX_60m | Patraix | - | ❌ New station |

## Weather Stations (5 current)
| FIWARE ID | Current Name | Historical Name | Has Historical Data |
|-----------|--------------|-----------------|---------------------|
| W01_AVFRANCIA_10m | Avd. Francia | Avda. Francia | ✅ Yes |
| W02_NAZARET_10m | Nazaret | Nazaret Meteo | ✅ Yes |
| W03_VALENCIAAEROPUERTO_10m | Aeropuerto | - | ❌ New (AEMET 8414A available) |
| W04_VALENCIADT_10m | Jardines de Viveros | Viveros | ✅ Yes (coordinate match) |
| W05_VALENCIA_UPV_10m | UPV | Politecnico | ✅ Yes (coordinate match) |

## Coordinate-Based Matches
Analysis showed that some current stations have identical coordinates to historical ones:
- **W04_VALENCIADT_10m** (39.48°, -0.37°) = Historical "Viveros" (0m distance)
- **W05_VALENCIA_UPV_10m** (39.48°, -0.34°) = Historical "Politecnico" (0m distance)

## Historical-Only Stations (not in current data)
These stations appear in RVVCCA historical data but don't have current FIWARE equivalents:
- **Conselleria Meteo** - Discontinued weather station
- **Puerto Moll Trans. Ponent** - Port area station
- **Puerto Valencia** - Port area station  
- **Puerto llit antic Turia** - Port area station

## Data Sources
- **Current stations**: Valencia Open Data API (`estacions-contaminacio-atmosferiques`, `estacions-atmosferiques`)
- **Historical data**: RVVCCA datasets (`rvvcca`, `rvvcca_d_horarios_2016-2020`, `rvvcca_d_horarios_2021-2022`)
- **Alternative for W03 (Airport)**: AEMET station 8414A (requires API key)

## Database Files
- `db/init/030-consolidate-station-ids.sql` - Migration script for consolidating station IDs
- `backfill/load_historical.py` - Python script for loading historical CSV data

## Station Coordinates (Current API)
```
# Air Quality
A01: 39.4578, -0.3430  | A02: 39.4504, -0.3963  | A03: 39.4811, -0.4088
A04: 39.4581, -0.3766  | A05: 39.4796, -0.3374  | A06: 39.4796, -0.3696
A07: 39.4705, -0.3764  | A08: 39.4667, -0.3283  | A09: 39.4744, -0.3285
A10: 39.4692, -0.4059  | A11: 39.4592, -0.4014

# Weather
W01: 39.4575, -0.3427  | W02: 39.4485, -0.3333  | W03: 39.4900, -0.4700
W04: 39.4800, -0.3700  | W05: 39.4800, -0.3400
```
