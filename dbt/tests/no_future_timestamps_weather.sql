-- Failing rows: weather readings with timestamps in the future.
-- Derived from direct_sql_queries.txt check #8.

select *
from {{ source('weather', 'hyper') }}
where ts > now()
