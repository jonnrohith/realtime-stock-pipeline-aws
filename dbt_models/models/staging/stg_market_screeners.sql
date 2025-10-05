-- Staging model for market screeners
-- Cleans and standardizes market screener data

with source_data as (
    select * from {{ source('raw_financial_data', 'market_screeners') }}
),

cleaned_screeners as (
    select
        symbol,
        name,
        price,
        change,
        change_percent,
        volume,
        market_cap,
        screener_type,
        rank,
        message_timestamp,
        job_id,
        source
    from source_data
    where 
        symbol is not null
        and screener_type is not null
),

enriched_screeners as (
    select
        *,
        -- Calculate performance categories
        case 
            when change_percent > 10 then 'Outstanding'
            when change_percent > 5 then 'Excellent'
            when change_percent > 0 then 'Good'
            when change_percent > -5 then 'Poor'
            else 'Terrible'
        end as performance_category,
        
        -- Calculate market cap category
        case 
            when market_cap >= 200000000000 then 'Mega Cap'
            when market_cap >= 10000000000 then 'Large Cap'
            when market_cap >= 2000000000 then 'Mid Cap'
            when market_cap >= 300000000 then 'Small Cap'
            else 'Micro Cap'
        end as market_cap_category,
        
        -- Add data quality flags
        case 
            when price is null or price <= 0 then false
            when change_percent is null then false
            else true
        end as is_valid_screener
        
    from cleaned_screeners
)

select * from enriched_screeners
