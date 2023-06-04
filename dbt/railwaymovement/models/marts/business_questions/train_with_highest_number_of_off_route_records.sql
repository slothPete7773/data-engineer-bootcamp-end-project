with source as (
    select 
        *
    from {{ ref('fact_movements') }}
)
select 
    train_id
    , count(1) as record_count
from source
where variation_status='OFF ROUTE'
group by 1
order by 2 desc

