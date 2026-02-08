-- Failing rows: air readings with timestamps in the future.
-- Derived from direct_sql_queries.txt check #8.

select *
from {{ source('air', 'hyper') }}
where ts > now()
