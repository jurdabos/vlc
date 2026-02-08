-- Failing rows: air_quality_summary values not in the expected set.
-- Derived from direct_sql_queries.txt check #4.

select *
from {{ source('air', 'hyper') }}
where air_quality_summary is not null
  and air_quality_summary not in (
      'Buena',
      'Admisible',
      'Mejorable',
      'Mala',
      'Muy mala',
      'Razonablemente Buena',
      'Hazardous',
      'Desfavorable',
      'Regular'
  )
