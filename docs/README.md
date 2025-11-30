docs/README.md

\# Project goals, use cases, glossary



IMAGINATIVE TASK CONTEXT

We work for the municipality of VLC that has installed sensors to measure environmental metrics and weather data. The project's overall goal is to provide relevant stakeholders with better information to improve the city's environmental conditions in the long term. The project started a couple of decades ago, and so far, some of the sensors have been installed. A chunk of data has already been collected. For us, the goal is to design and implement a data processing system that reliably stores relevant info putting more emphasis on front-end applications planned to be developed. We have designed a data system capable of storing and processing a continuous sensor data stream, using Kafka to stream the data to TimescaleDB.



CONCEPTION

We have created a written concept at "\\docs\\concept\_note.pdf" to describe everything that belongs to the data system.



OVERALL DATA PICTURE

VLC in the 2020s is collecting a rich set of environmental metrics citywide, which cover air pollution, weather conditions, and noise levels, among others. Per the website of the municipality, the sensors are set up to provide actionable insights: city planners should be able to identify pollution hotspots or noisy streets and craft targeted interventions.



DETAILS

Valencia’s Smart City Office has implemented an integrated network of IoT sensors and a central data platform (VLCi) to collect data from multiple sources. We can consume atmospheric contamination information.

https://valencia.opendatasoft.com/explore/dataset/estacions-contaminacio-atmosferiques-estaciones-contaminacion-atmosfericas/api/

We can have a look at contamination data on a map to get an initial bearing on where the measurement locations are.

https://geoportal.valencia.es/apps/GeoportalHome/es/inicio/contaminacion-atmosferica-y-ruido

We can familiarize ourselves with data formats used by the municipality.

https://valencia.opendatasoft.com/pages/que\_son/

It is possible to look around in 270+ databases maintained by the municipality.

https://valencia.opendatasoft.com/explore/?disjunctive.features\&disjunctive.modified\&disjunctive.publisher\&disjunctive.keyword\&disjunctive.theme\&disjunctive.language\&sort=modified

There were 40+ devices installed on municipal EMT buses to capture air quality data along bus routes, together with temperature and humidity readings.

https://www.valencia.es/web/smartcity/cas/proyectos/sensores-medioambientales-embarcados-emt

These currently seem to be unavailable for data consumption.

The city has deployed noise sensors in tourist areas. These sensors log the equivalent continuous sound level, or specifically the Level A-weighted equivalent (LAeq) and produce daily noise indicators for day, evening, and night periods.

Valencia also monitors climatic parameters and environmental hazards. IoT sensors track rainfall and river water levels, feeding data into AI models that predict flood risks (https://thinkz.ai/smart-cities-trends-2025-ai-iot/#:~:text=Disaster%20prevention%20and%20response%20will,warnings%20to%20help%20reduce%20damage). Given the Mediterranean climate and occasional heavy rainstorms, these sensors provide early warning of floods. Additionally, standard weather sensors are present in both official weather stations and some IoT nodes, helping to map the urban microclimate.

The city’s broader smart-city strategy includes related sensor deployments. Waste containers are equipped with fill-level sensors to prevent overflow and optimize collection routes. Smart parking sensors on streets help reduce traffic circling. Furthermore, Valencia leverages data from energy consumption sensors in buildings and even experiments with energy poverty IoT sensors to improve environmental equity. All sensor data is funneled into the central VLCi platform for unified management and analysis.



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

VLC integrates data from rain gauges and river level sensors to manage flood risk, an increasingly important metric with climate change. When rainfall intensity or river height crosses a threshold, the system can alert emergency services and the public. Wind speed sensors (on weather masts or building tops) might also be part of the network, aiding in air pollution dispersion modeling and providing warnings for high-wind events. All these metrics give a comprehensive picture of the city’s environmental conditions, beyond just pollution.



SCALE OF DATA

For a city the size of Valencia, the sensor deployment as of 2025 can be considered moderate, but it is growing. The pilot phases involve on the order of dozens of sensors, each streaming data at high frequency. The real-time data approach (“València al Minut”) means data is continuously fed into VLCi.





REASONS FOR CHOOSING TIMESCALEDB FOR OUR SOLUTION

* PostgreSQL with TS superpowers + perfect with PostGIS for spatial queries
* capable to join raw readings w/ rich metadata (later-phase device ↔ bus ↔ route ↔ neighbourhood) \& do windowed rollups, + map overlays
* sits behind a Kafka sink (pgJDBC), giving us a Lambda/Kappa-ish pipeline: speed in via Kafka, durable store in ts, serve aggs to grafana dashboards
* it inherits mature SQL-level RBAC, row/column policies, encryption options, \& audit patterns from pg, all of which can come in handy when we need to demonstrate purpose limitation, data minimization, storage limitation, \& confidentiality across the stack to match the EU frameworks we have to respect in VLC
* pg-based documentation is abundant -> reliable DataOps gets a chance
