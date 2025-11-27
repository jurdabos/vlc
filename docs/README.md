docs/README.md

\# Project goals, use cases, glossary





IMAGINATIVE TASK CONTEXT

We work for the municipality of VLC that has installed sensors to measure environmental metrics and weather data. The project's overall goal is to provide relevant personnel with better information to improve the city's environmental conditions in the long term. Also, the data will be used for developing an application that warns citizens if measures differ from recommended values. The project started in 2014, and so far, some of the sensors have been installed. A big chunk of data has already been collected. For us, the goal is to design and implement a data processing system that reliably stores the data making it accessible and usable by more front-end applications planned to be developed. We will need to design and implement a data system capable of storing and processing a continuous sensor data stream. We will use Kafka to stream the data to TimescaleDB.





CONCEPTION

We have created a written concept at "\\docs\\concept\_note.pdf" to describe everything that belongs to the data system.





OVERALL DATAING PICTURE

VLC in 2025 is collecting a rich set of environmental metrics citywide. The metrics cover air pollution (particulates and key gases), weather conditions (temperature, humidity, etc.), and noise levels, among others. Per the website of the municipality, the sensors are set up to provide actionable insights: city planners should be able to identify pollution hotspots or noisy streets and craft targeted interventions. Also, citizens can receive timely warnings (e. g. poor air quality advisories or heat alerts) through notifications from a mobile application that are based on live data.





DETAILS

As of 2025, the municipality has launched projects to gather real-time data on air quality, weather, and noise pollution.



Smart City Sensor Initiatives in Valencia

Valencia’s Smart City Office has implemented an integrated network of IoT sensors and a central data platform (called the VLCi platform) to collect data from multiple sources. We can consume atmospheric contamination data through an API.

https://valencia.opendatasoft.com/explore/dataset/estacions-contaminacio-atmosferiques-estaciones-contaminacion-atmosfericas/api/

We can have a look at contamination data on a map just to get an initial bearing on where the measurement locations are.

https://geoportal.valencia.es/apps/GeoportalHome/es/inicio/contaminacion-atmosferica-y-ruido

We can familiarize ourselves with data formats used by the municipality.

https://valencia.opendatasoft.com/pages/que\_son/

There were 40+ devices installed on municipal EMT buses to capture air quality data along bus routes, together with temperature and humidity readings. These moving sensors greatly increase coverage beyond the 6 official air-quality stations currently operated by the regional government in the city. By using 44 hybrid/electric buses as sensor carriers, Valencia obtains real-time information throughout the city on air quality, temperature, humidity, and sound levels. This data feeds into Valencia’s central platform to help calculate an up-to-date Air Quality Index (AQI) for each area of the city.

https://www.valencia.es/web/smartcity/cas/proyectos/sensores-medioambientales-embarcados-emt

It is possible to look around in 270+ databases maintained by the municipality.

https://valencia.opendatasoft.com/explore/?disjunctive.features\&disjunctive.modified\&disjunctive.publisher\&disjunctive.keyword\&disjunctive.theme\&disjunctive.language\&sort=modified

Fixed Noise Monitoring Stations: The city has deployed noise sensors in tourist areas (in designated noise pollution zones). These sensors log the equivalent continuous sound level, or specifically the Level A-weighted equivalent (LAeq) and produce daily noise indicators for day, evening, and night periods. Valencia’s open data portal publishes noise data from such sensors, reflecting an ongoing effort to monitor acoustic pollution in busy nightlife or traffic areas.



Weather and Flood Sensors

Valencia also monitors climatic parameters and environmental hazards. IoT sensors track rainfall and river water levels, feeding data into AI models that predict flood risks (https://thinkz.ai/smart-cities-trends-2025-ai-iot/#:~:text=Disaster%20prevention%20and%20response%20will,warnings%20to%20help%20reduce%20damage). Given the Mediterranean climate and occasional heavy rainstorms, these sensors provide early warning of floods. Additionally, standard weather sensors (temperature, humidity, wind, barometric pressure) are present in both official weather stations and some IoT nodes, helping to map the urban microclimate.



Other Smart Environmental Measures

The city’s broader smart-city strategy includes related sensor deployments. Waste containers are equipped with fill-level sensors to prevent overflow and optimize collection routes. Smart parking sensors on streets help reduce traffic circling (indirectly lowering emissions). While these are not environmental metrics per se, they contribute to sustainability. Furthermore, Valencia leverages data from energy consumption sensors in buildings and even experiments with energy poverty IoT sensors (e. g. via the Helium network) to improve environmental equity. All sensor data is funneled into the central VLCi platform for unified management and analysis.





KEY ENV METRICS MONITORED

1\. **Air Quality** (Pollutants)

A primary focus is on measuring air pollution levels. This includes fine particulate matter – PM₂.₅ and PM₁₀ (“fine dust”) – which affect respiratory health. Mobile and static sensors are equipped with optical particle counters to estimate PM concentrations in the air. In addition, gaseous pollutants are monitored where possible: for instance, multiple stations measure nitrogen dioxide (NO₂) and ground-level ozone (O₃), key traffic and smog pollutants (https://www.mdpi.com/1424-8220/23/23/9585#:~:text=official%20AQ%20monitoring%20stations%20,to%20enhance%20depend%20on%20many). Some sensor units also detect volatile organic compounds (VOCs) or even specific gases like formaldehyde (CH₂O), as these contribute to overall air quality. Valencia has only six fixed stations for a city of ~800k people, so supplementing with dozens of smaller sensors should address the gap. The data is used to compute an Air Quality Index and identify hotspots of pollution in near real time.

2\. **Carbon Dioxide** (CO₂)

Valencia’s initiative includes CO₂ sensors as part of its environmental packages. Monitoring CO₂ serves two purposes: (1) as a proxy for combustion-related activity in busy urban areas, and (2) for indoor air quality and energy efficiency projects. Modern NDIR CO₂ sensors are affordable and have been integrated in pilot sensor nodes, achieving good accuracy (one study in VLC reported CO₂ measurement errors under 1% after calibration). Cf. https://ouci.dntb.gov.ua/en/works/4MwZY0b9/#:~:text=prediction%20of%20the%20readings%2C%20we,7.

3\. **Temperature and Humidity**

Multiple sensor nodes log ambient temperature (°C) and relative humidity. These basic metrics help in understanding the urban heat island effect and comfort levels across different neighborhoods. Valencia’s buses carrying sensors, for example, continuously report temperature and humidity as they move through the city. Given VLC's warm climate, having granular temperature/humidity data is invaluable for public health (e. g. warning citizens of heat index extremes) and for energy planning.

4\. **Noise Levels**

Environmental noise is monitored via sound level sensors. VLC has placed acoustic meters in areas with active nightlife, recording noise in decibels (dB). These devices compute LAeq (the equivalent continuous sound level) over a short interval (e. g. 1-minute LAeq) to capture fluctuations (https://valencia.opendatasoft.com/explore/dataset/dades-diaries-del-sensor-de-soroll-ubicat-al-barri-de-russafa-en-el-carrer-salva/table/). By aggregating these, the city evaluates compliance with noise regulations – for instance, whether night-time noise in entertainment districts stays below legal limits.

5\. **Weather \& Hydrology**

VLC integrates data from rain gauges and river level sensors to manage flood risk, an increasingly important metric with climate change. When rainfall intensity or river height crosses a threshold, the system can alert emergency services and the public. Wind speed sensors (on weather masts or building tops) might also be part of the network, aiding in air pollution dispersion modeling and providing warnings for high-wind events. All these metrics give a comprehensive picture of the city’s environmental conditions, beyond just pollution.





SCALE OF DATA

For a city the size of Valencia, the sensor deployment as of 2025 can be considered moderate, but it is definitely growing. The pilot phases involve on the order of dozens of sensors, each streaming data at high frequency. The real-time data approach (“València al Minut”) means data is continuously fed into the platform. VLC's open-data portal hosts millions of records from such sensors.





WHY TIMESCALEDB?

* PostgreSQL with TS superpowers (hypertables, retention, compression, continuous aggregates) + perfect with PostGIS for spatial queries
* capable to join raw readings w/ rich metadata (device ↔ bus ↔ route ↔ neighbourhood) \& do windowed rollups (1-min LAeq, 5-min aggs), + map overlays
* sits neatly behind a Kafka sink (pgJDBC), giving us a Lambda/Kappa-ish pipeline: speed in via Kafka, durable store in Timescale, serve aggs to apps/dashboards
* it inherits mature SQL-level RBAC, row/column policies, encryption options, \& audit patterns from pg, all of which can come in quite handy when we need to demonstrate purpose limitation, data minimization, storage limitation, \& confidentiality across the stack to match the EU framing we have to respect in VLC

pg-based documentation and skills are abundant; CI/CD, migrations, backups, and metrics are predictable -> reliability/MTTR/MTBF targets in DataOps

