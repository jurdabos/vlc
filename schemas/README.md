\# schemas/\*.avsc are ramp-up option notes for future Avro seriousness (P3/4/5)

\## Avro vs JSON Schema: Assessment for VLC



\### Current State

We are using \*\*JSON Schema\*\* with Schema Registry (`producer/schemas/air.json`). The `schemas/\*.avsc` files are placeholders for Avro migration for the future.



\### Benefits of Avro Over JSON Schema



| Aspect 			| JSON Schema (current) 			| Avro 					|

|-------------------------------|-----------------------------------------------|---------------------------------------|

| \*\*Message size\*\* 		| ~2-3x larger (field names in every message) 	| Compact binary, ~40-60% smaller 	|

| \*\*Serialization speed\*\* 	| Slower 					| ~2-5x faster 				|

| \*\*Schema evolution\*\* 		| Supported but less strict 			| Stronger compatibility guarantees 	|

| \*\*Tooling\*\* 			| Good 						| Excellent (Kafka ecosystem native) 	|



\*\*For ~11 stations, polling every 5 min: The throughput is low enough that JSON Schema works fine.

Avro benefits become significant at high throughput.



\### Complexity of Migration: \*\*Medium\*\*



\### What Needs to Change



\*\*1. Schema files\*\* (`schemas/air.avsc`, `schemas/weather.avsc`) ♥

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



\### Decision on 2025-12-19

We are still contemplating if we should go with Avro.

