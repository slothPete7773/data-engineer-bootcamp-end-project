with src as (
    select 
        *   
    from {{ ref('fact_movements') }}
)
, compute as (
    select 
        company_name
        , variation_status
        , count(1) as count_record
    from src
    where
        variation_status='ON TIME'
        and date(actual_timestamp_utc) >= date_add(current_date(), interval -3 day)
    group by 1, 2
    order by 3 desc 
    limit 1
)
select * from compute