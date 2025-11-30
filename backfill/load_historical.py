"""
Transforms and loads historical RVVCCA data into TimescaleDB.
"""
import csv
import os
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import execute_values

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "vlc")
PG_USER = os.getenv("PG_USER", "vlc_dev")
PG_PASSWORD = os.getenv("PG_PASSWORD", "itt-csak")

# Mapping historical station names to current fiwareids
STATION_TO_AIR_FIWAREID = {
    "Avda. Francia": "A01_AVFRANCIA_60m",
    "Bulevard Sud": "A02_BULEVARDSUD_60m",
    "Moli del Sol": "A03_MOLISOL_60m",
    "Pista Silla": "A04_PISTASILLA_60m",
    "Politecnico": "A05_POLITECNIC_60m",
    "Viveros": "A06_VIVERS_60m",
    "Valencia Centro": "A07_VALENCIACENTRE_60m",
    "Valencia Olivereta": "A10_OLIVERETA_60m",
}

STATION_TO_WEATHER_FIWAREID = {
    "Avda. Francia": "W01_AVFRANCIA_10m",
    "Nazaret Meteo": "W02_NAZARET_10m",
    "Viveros": "W04_VALENCIADT_10m",  # Same coords as current "Jardines de Viveros"
    "Politecnico": "W05_VALENCIA_UPV_10m",  # Same coords as current UPV station
}

# Station coordinates (from current API data, November 2025)
STATION_COORDS = {
    # Air quality stations
    "Avda. Francia": (39.45782688751831, -0.342986232422652),
    "Bulevard Sud": (39.45039600550536, -0.3963375643758562),
    "Moli del Sol": (39.48111211090413, -0.4088098969009376),
    "Pista Silla": (39.45806095369672, -0.37664393657915707),
    "Politecnico": (39.47964449692915, -0.33740066052186946),
    "Viveros": (39.47964092480533, -0.36964822314381013),
    "Valencia Centro": (39.470547670260125, -0.37639765165532396),
    "Valencia Olivereta": (39.46924423509195, -0.40592344552906795),
    # Weather stations
    "Nazaret Meteo": (39.4485309997218, -0.3332980005434063),
}


def parse_float(val):
    """Parses a float value, returns None if empty or invalid."""
    if val is None or val == "" or val == "null":
        return None
    try:
        return float(val)
    except ValueError:
        return None


def parse_timestamp(fecha, hora):
    """Combines date and time into a timestamp."""
    if not fecha:
        return None
    try:
        if hora:
            dt_str = f"{fecha}T{hora}"
            dt = datetime.fromisoformat(dt_str)
        else:
            dt = datetime.fromisoformat(fecha)
        return dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def load_air_data(conn, csv_path, batch_size=5000):
    """Loads air quality data from historical CSV."""
    print(f"[air] Loading from {csv_path}")
    sql = """
        INSERT INTO air.hyper (fiwareid, ts, no2, o3, so2, co, pm10, pm25, lat, lon)
        VALUES %s
        ON CONFLICT (fiwareid, ts) DO UPDATE SET
            no2 = EXCLUDED.no2,
            o3 = EXCLUDED.o3,
            so2 = EXCLUDED.so2,
            co = EXCLUDED.co,
            pm10 = EXCLUDED.pm10,
            pm25 = EXCLUDED.pm25,
            lat = EXCLUDED.lat,
            lon = EXCLUDED.lon
    """
    total = 0
    skipped = 0
    batch = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            station = row.get("estacion", "")
            fiwareid = STATION_TO_AIR_FIWAREID.get(station)
            if not fiwareid:
                skipped += 1
                continue
            ts = parse_timestamp(row.get("fecha"), row.get("hora"))
            if not ts:
                skipped += 1
                continue
            coords = STATION_COORDS.get(station, (None, None))
            batch.append((
                fiwareid,
                ts,
                parse_float(row.get("no2")),
                parse_float(row.get("o3")),
                parse_float(row.get("so2")),
                parse_float(row.get("co")),
                parse_float(row.get("pm10")),
                parse_float(row.get("pm2_5")),
                coords[0],
                coords[1],
            ))
            if len(batch) >= batch_size:
                with conn.cursor() as cur:
                    execute_values(cur, sql, batch)
                conn.commit()
                total += len(batch)
                print(f"[air] Inserted {total} records...")
                batch = []
    # Inserting remaining
    if batch:
        with conn.cursor() as cur:
            execute_values(cur, sql, batch)
        conn.commit()
        total += len(batch)
    print(f"[air] Done. Total: {total}, Skipped: {skipped}")
    return total


def load_weather_data(conn, csv_path, batch_size=5000):
    """Loads weather data from historical CSV."""
    print(f"[weather] Loading from {csv_path}")
    sql = """
        INSERT INTO weather.hyper (fiwareid, ts, wind_dir_deg, wind_speed_ms, temperature_c, humidity_pct, pressure_hpa, precip_mm, lat, lon)
        VALUES %s
        ON CONFLICT (fiwareid, ts) DO UPDATE SET
            wind_dir_deg = EXCLUDED.wind_dir_deg,
            wind_speed_ms = EXCLUDED.wind_speed_ms,
            temperature_c = EXCLUDED.temperature_c,
            humidity_pct = EXCLUDED.humidity_pct,
            pressure_hpa = EXCLUDED.pressure_hpa,
            precip_mm = EXCLUDED.precip_mm,
            lat = EXCLUDED.lat,
            lon = EXCLUDED.lon
    """
    total = 0
    skipped = 0
    batch = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            station = row.get("estacion", "")
            fiwareid = STATION_TO_WEATHER_FIWAREID.get(station)
            if not fiwareid:
                skipped += 1
                continue
            ts = parse_timestamp(row.get("fecha"), row.get("hora"))
            if not ts:
                skipped += 1
                continue
            # Skipping rows without any weather data
            temp = parse_float(row.get("temperatura"))
            if temp is None:
                skipped += 1
                continue
            coords = STATION_COORDS.get(station, (None, None))
            batch.append((
                fiwareid,
                ts,
                parse_float(row.get("direccion_del_viento")),
                parse_float(row.get("velocidad_del_viento")),
                temp,
                parse_float(row.get("humedad_relativa")),
                parse_float(row.get("presion")),
                parse_float(row.get("precipitacion")),
                coords[0],
                coords[1],
            ))
            if len(batch) >= batch_size:
                with conn.cursor() as cur:
                    execute_values(cur, sql, batch)
                conn.commit()
                total += len(batch)
                print(f"[weather] Inserted {total} records...")
                batch = []
    # Inserting remaining
    if batch:
        with conn.cursor() as cur:
            execute_values(cur, sql, batch)
        conn.commit()
        total += len(batch)
    print(f"[weather] Done. Total: {total}, Skipped: {skipped}")
    return total


def main():
    """Main backfill function."""
    import sys
    print(f"Connecting to {PG_HOST}:{PG_PORT}/{PG_DB}")
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )
    # Using command line arg or default
    csv_file = sys.argv[1] if len(sys.argv) > 1 else "hourly_2021_2022.csv"
    if not os.path.exists(csv_file):
        print(f"ERROR: {csv_file} not found!")
        return
    load_air_data(conn, csv_file)
    load_weather_data(conn, csv_file)
    conn.close()
    print("Backfill complete!")


if __name__ == "__main__":
    main()
