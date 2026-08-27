-- A trip tagged 2024-01 must have actually occurred in January 2024.
-- Catches mislabeled loads and TLC's occasional stray timestamps.

select
    source_month,
    count(*) as offending_rows
from {{ ref('stg_trips') }}
where to_char(pickup_date, 'YYYY-MM') != source_month
group by 1