
\## Avro vs JSON Schema for VLC



\### Current State

We are using \*\*JSON Schema\*\* with Schema Registry (`producer/schemas/air.json`). The `schemas/\*.avsc` files are ramp-up option notes for Avro migration to be done in current P3.




\### Benefits of Avro Over JSON Schema



| Aspect 				| JSON Schema (current) 			| Avro 					|

|---------------------------------------|-----------------------------------------------|---------------------------------------|

| \*\*Message size\*\* 			| ~2-3x larger (field names in every message) 	| Compact binary, ~40-60% smaller 	|

| \*\*Serialization speed\*\* 		| Slower 					| ~2-5x faster 				|

| \*\*Schema evolution\*\* 		| Supported but less strict 			| Stronger compatibility guarantees 	|

| \*\*Tooling\*\* 			| Good 						| Excellent (Kafka ecosystem native) 	|



\*\*For ~11 stations, polling every 5 min: The throughput is low enough that JSON Schema works fine, but we want to experiment with 
Avro for significant benefits at higher throughput.



\### What Needs to Change



\*\*1. Schema files\*\* (`schemas/air.avsc`, `schemas/weather.avsc`) ♥ Done

\*\*2. Producer changes\*\* (`air\_producer.py`):

```python

\# Replace JSONSerializer with AvroSerializer

from confluent\_kafka.schema\_registry.avro import AvroSerializer

\# Load .avsc instead of .json

SCHEMA\_PATH = Path(\_\_file\_\_).parent.parent / "schemas" / "air.avsc"

\# Change serializer instantiation

avro\_serializer = AvroSerializer(schema\_registry\_client, AIR\_SCHEMA\_STR)

```

\*\*3. Connector configs\*\* (`jdbc-sink.timescale.air.json`):

```json

"value.converter": "io.confluent.connect.avro.AvroConverter"

```

\*\*4. Dependencies\*\*: Add `fastavro` or use confluent-kafka's built-in Avro support (already included).

\*\*5. Timestamp handling\*\*: Avro uses `timestamp-millis` (epoch ms) vs ISO strings — requires conversion logic.



\### Decision on 2025-12-21

We have created a pre-migration commit, and we are launching on.

