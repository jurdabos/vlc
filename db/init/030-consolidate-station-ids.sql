-- 030-consolidate-station-ids.sql
-- Consolidates historical station IDs to match current FIWARE IDs
-- Run this AFTER the initial schema setup and BEFORE loading historical data

-- =============================================================================
-- AIR QUALITY STATIONS MAPPING
-- =============================================================================
-- Current (FIWARE)          | Historical Name      | Notes
-- --------------------------|----------------------|------------------------------
-- A01_AVFRANCIA_60m         | Avda. Francia        | ✓ Already mapped
-- A02_BULEVARDSUD_60m       | Bulevard Sud         | ✓ Already mapped
-- A03_MOLISOL_60m           | Moli del Sol         | ✓ Already mapped
-- A04_PISTASILLA_60m        | Pista Silla          | ✓ Already mapped
-- A05_POLITECNIC_60m        | Politecnico          | ✓ Already mapped
-- A06_VIVERS_60m            | Viveros              | ✓ Already mapped
-- A07_VALENCIACENTRE_60m    | Valencia Centro      | ✓ Already mapped
-- A08_DR_LLUCH_60m          | -                    | no historical
-- A09_CABANYAL_60m          | -                    | no historical
-- A10_OLIVERETA_60m         | Valencia Olivereta   | ✓ Already mapped
-- A11_PATRAIX_60m           | -                    | no historical

-- =============================================================================
-- WEATHER STATIONS MAPPING
-- =============================================================================
-- Current (FIWARE)          | Historical Name      | Notes
-- --------------------------|----------------------|------------------------------
-- W01_AVFRANCIA_10m         | Avda. Francia        | ✓ Already mapped
-- W02_NAZARET_10m           | Nazaret Meteo        | ✓ Already mapped
-- W03_VALENCIAAEROPUERTO_10m| -                    | no historical (AEMET 8414A possible)
-- W04_VALENCIADT_10m        | Viveros              | ✓ COORD MATCH (Jardines de Viveros)
-- W05_VALENCIA_UPV_10m      | Politecnico          | ✓ COORD MATCH (UPV campus)

-- =============================================================================
-- MIGRATE OLD FIWARE IDs TO CORRECT ONES
-- =============================================================================

-- If any data was loaded with incorrect IDs from previous mappings, fix them
-- W06_VIVERS_10m → W04_VALENCIADT_10m (same location, Viveros/Jardines de Viveros)
UPDATE weather.hyper
SET fiwareid = 'W04_VALENCIADT_10m'
WHERE fiwareid = 'W06_VIVERS_10m';

-- W07_VALENCIACENTRE_10m - this doesn't exist in current data, drop if exists
-- (Valencia Centro only has air quality station A07, not weather)
DELETE FROM weather.hyper
WHERE fiwareid = 'W07_VALENCIACENTRE_10m';

-- W_CONSELLERIA_10m - not a current station (Conselleria Meteo was historical only)
DELETE FROM weather.hyper
WHERE fiwareid = 'W_CONSELLERIA_10m';

-- =============================================================================
-- STATION REFERENCE TABLE (optional, for documentation)
-- =============================================================================
CREATE TABLE IF NOT EXISTS public.station_mapping (
    fiwareid        text PRIMARY KEY,
    station_type    text NOT NULL CHECK (station_type IN ('air', 'weather')),
    display_name    text NOT NULL,
    historical_name text,
    lat             double precision,
    lon             double precision,
    is_active       boolean DEFAULT true,
    notes           text
);

-- Upsert station reference data
INSERT INTO public.station_mapping (fiwareid, station_type, display_name, historical_name, lat, lon, is_active, notes)
VALUES
    -- Air quality stations
    ('A01_AVFRANCIA_60m', 'air', 'Francia', 'Avda. Francia', 39.45782688751831, -0.342986232422652, true, NULL),
    ('A02_BULEVARDSUD_60m', 'air', 'Boulevar Sur', 'Bulevard Sud', 39.45039600550536, -0.3963375643758562, true, NULL),
    ('A03_MOLISOL_60m', 'air', 'Molí del Sol', 'Moli del Sol', 39.48111211090413, -0.4088098969009376, true, NULL),
    ('A04_PISTASILLA_60m', 'air', 'Pista de Silla', 'Pista Silla', 39.45806095369672, -0.37664393657915707, true, NULL),
    ('A05_POLITECNIC_60m', 'air', 'Universidad Politécnica', 'Politecnico', 39.47964449692915, -0.33740066052186946, true, NULL),
    ('A06_VIVERS_60m', 'air', 'Viveros', 'Viveros', 39.47964092480533, -0.36964822314381013, true, NULL),
    ('A07_VALENCIACENTRE_60m', 'air', 'Centro', 'Valencia Centro', 39.470547670260125, -0.37639765165532396, true, NULL),
    ('A08_DR_LLUCH_60m', 'air', 'Dr. Lluch', NULL, 39.46668475546108, -0.3282894894027388, true, 'New station, no historical data'),
    ('A09_CABANYAL_60m', 'air', 'Cabanyal', NULL, 39.47439078535677, -0.32853481349274394, true, 'New station, no historical data'),
    ('A10_OLIVERETA_60m', 'air', 'Olivereta', 'Valencia Olivereta', 39.46924423509195, -0.40592344552906795, true, NULL),
    ('A11_PATRAIX_60m', 'air', 'Patraix', NULL, 39.45918908999643, -0.4014113292191286, true, 'New station, no historical data'),
    -- Weather stations
    ('W01_AVFRANCIA_10m', 'weather', 'Avd. Francia', 'Avda. Francia', 39.457525999921394, -0.34266599991045765, true, NULL),
    ('W02_NAZARET_10m', 'weather', 'Nazaret', 'Nazaret Meteo', 39.4485309997218, -0.3332980005434063, true, NULL),
    ('W03_VALENCIAAEROPUERTO_10m', 'weather', 'Aeropuerto', NULL, 39.489999999779805, -0.4699999998718748, true, 'New station, AEMET 8414A available'),
    ('W04_VALENCIADT_10m', 'weather', 'Jardines de Viveros', 'Viveros', 39.479999999578624, -0.36999999999057726, true, 'Historical "Viveros" weather data'),
    ('W05_VALENCIA_UPV_10m', 'weather', 'UPV', 'Politecnico', 39.48000000035075, -0.3400000005661824, true, 'Historical "Politecnico" weather data')
ON CONFLICT (fiwareid) DO UPDATE SET
    display_name = EXCLUDED.display_name,
    historical_name = EXCLUDED.historical_name,
    lat = EXCLUDED.lat,
    lon = EXCLUDED.lon,
    is_active = EXCLUDED.is_active,
    notes = EXCLUDED.notes;

-- Grant access to vlc_dev
GRANT SELECT ON public.station_mapping TO vlc_dev;
