with movements as (
    select  
        event_type
        , actual_timestamp_utc
        , event_source
        , train_id
        , variation_status
        , toc_id
    from {{ ref('stg_networkrail__movements') }}
)
, companies as (
    select 
        toc_id
        , company_name
    from {{ ref('stg_networkrail__operating_companies') }}
)
, joined as (
    select 
        event_type
        , actual_timestamp_utc
        , event_source
        , train_id
        , variation_status
        , M.toc_id as toc_id
        , company_name
    from movements as M
    join companies as C
        on M.toc_id=C.toc_id
)
, final as (
    select * from joined
)
select * from final