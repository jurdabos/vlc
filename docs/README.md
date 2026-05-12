docs/README.md

\# Project goals, use cases, glossary



TASK CONTEXT

The municipality of VLC has installed sensors to measure environmental metrics and weather data. The project's overall goal is to provide stakeholders with better information to improve the city's environmental conditions in the long term. A substantial chunk of data has already been collected. For us, the goal is to design and implement a data processing system that reliably stores relevant info putting emphasis on sturdy up the backbones before front-end applications are developed. We have designed a data system capable of storing and processing a continuous sensor data stream, using Kafka to stream the data to TimescaleDB.



CONCEPTION and DEVELOPMENT

We have created a written concept at "\\docs\\concept\_note.pdf" to describe everything that belongs to the data system.

Also, in "\\docs\\dev\_notes.pdf", you can check information on the development process.



OVERALL DATA PICTURE

VLC in the 2020s is collecting a rich set of environmental metrics, which cover air pollution, weather conditions, and noise levels, among others. Per the website of the municipality, the sensors are set up to provide actionable insights: city planners should be able to identify pollution hotspots or noisy streets and craft targeted interventions.



DETAILS

Valencia’s Smart City Office has implemented an integrated network of IoT sensors and a central data platform (VLCi) to collect data from multiple sources. The streaming side of this project consumes the live atmospheric contamination + weather feeds via the city geoportal’s ArcGIS REST endpoint:

https://geoportal.valencia.es/server/rest/services/OPENDATA/MedioAmbiente/MapServer/156 (air pollution)
https://geoportal.valencia.es/server/rest/services/OPENDATA/MedioAmbiente/MapServer/157 (weather)

We can have a look at contamination data on a map to get a bearing on where the measurement locations are:

https://geoportal.valencia.es/apps/GeoportalHome/es/inicio/contaminacion-atmosferica-y-ruido

Note (2026-05): the legacy Opendatasoft host (`valencia.opendatasoft.com`, formerly served at `https://valencia.opendatasoft.com/explore/dataset/...`) was decommissioned. The historical RVVCCA-style backfill in `backfill/` was captured before the migration; new ingestion goes through the ArcGIS layers above. Field names (`fiwareid`, `fecha_carg`, pollutant + meteo columns) are unchanged.

There were 40+ devices installed on municipal EMT buses to capture air quality data along bus routes, together with temperature and humidity readings.

https://www.valencia.es/web/smartcity/cas/proyectos/sensores-medioambientales-embarcados-emt

These currently seem to be unavailable for data consumption. (clarification request sent to Contacta.vlci@valencia.es)

The city has deployed noise sensors in tourist areas. These sensors log the Level A-weighted equivalent (LAeq) and produce daily noise indicators for day, evening, and night periods.

VLC also monitors climatic parameters and environmental hazards. IoT sensors track rainfall and river water levels, feeding data into AI models that predict flood risks (https://thinkz.ai/smart-cities-trends-2025-ai-iot/#:~:text=Disaster%20prevention%20and%20response%20will,warnings%20to%20help%20reduce%20damage). Given the Mediterranean climate and occasional heavy rainstorms, these sensors provide early warning of floods.

The city’s broader smart-city strategy includes related sensor deployments. Waste containers are equipped with fill-level sensors to prevent overflow and optimize collection routes. Smart parking sensors on streets help reduce traffic circling. Furthermore, VLC leverages data from energy consumption sensors in buildings and even experiments with energy poverty IoT sensors to improve environmental equity. All sensor data is funneled into the central VLCi platform for unified management and analysis.



KEY ENV METRICS MONITORED

1\. **Air Quality**

Air pollution levels measured include fine particulate matter – PM₂.₅ and PM₁₀ – which affect respiratory health. Mobile and static sensors are equipped with optical particle counters to estimate PM concentrations in the air. In addition, gaseous pollutants are monitored where possible: for instance, multiple stations measure nitrogen dioxide (NO₂) and ground-level ozone (O₃), key traffic and smog pollutants (https://www.mdpi.com/1424-8220/23/23/9585#:~:text=official%20AQ%20monitoring%20stations%20,to%20enhance%20depend%20on%20many). Some sensor units also detect volatile organic compounds (VOCs) or even specific gases like formaldehyde (CH₂O), as these contribute to overall air quality.

2\. **Carbon Dioxide** (CO₂)

The initiative includes CO₂ sensors as part of the environmental package. Monitoring CO₂ serves two purposes: (1) a proxy for combustion-related activity in busy urban areas, and (2) for indoor air quality and energy efficiency projects. Modern NDIR CO₂ sensors are affordable and have been integrated in pilot sensor nodes, achieving good accuracy (one study in VLC reported CO₂ measurement errors under 1% after calibration). Cf. https://ouci.dntb.gov.ua/en/works/4MwZY0b9/#:~:text=prediction%20of%20the%20readings%2C%20we,7.

3\. **Temperature and Humidity**

Multiple sensor nodes log ambient temperature and relative humidity. These basic metrics help in understanding the urban heat island effect and comfort levels across different neighborhoods. Given VLC's warm climate, having granular temperature/humidity data is invaluable for public health and for energy planning.

4\. **Noise Levels**

Environmental noise is monitored via sound level sensors. VLC has placed acoustic meters in areas with active nightlife, recording noise in decibels (dB). These devices compute LAeq  over a short interval to capture fluctuations (https://valencia.opendatasoft.com/explore/dataset/dades-diaries-del-sensor-de-soroll-ubicat-al-barri-de-russafa-en-el-carrer-salva/table/). By aggregating these, the city evaluates compliance with noise regulations.

5\. **Weather \& Hydrology**

VLC integrates data from rain gauges and river level sensors to manage flood risk, an increasingly important metric with climate change. When rainfall intensity or river height crosses a threshold, the system can alert emergency services and the public.



SCALE OF DATA

For a city the size of Valencia, the sensor deployment as of 2025 can be considered moderate, but growing. The real-time data approach (“València al Minut”) means data is continuously fed into VLCi.



WHY TIMESCALEDB?

* PostgreSQL with TS superpowers \& PostGIS for spatial queries
* capable to join raw readings w/ rich metadata (later-phase device ↔ bus ↔ route ↔ neighbourhood) \& do windowed rollups + map overlays
* sits behind a Kafka sink (pgJDBC), giving a Λ/Κ-ish pipeline: speed in via Kafka, durable store in ts, serve aggs to grafana dashboards
* inherits mature SQL-level RBAC, row/column policies, encryption options, \& audit patterns from pg (handy when in need to demonstrate purpose limitation, data minimization, storage limitation, \& confidentiality across the stack to match EU frameworks we have to respect in VLC)
* pg-based documentation is abundant -> higher chance of reliable DataOps
