-- Staging model for stock quotes
-- Cleans and standardizes raw stock quote data

with source_data as (
    select * from {{ source('raw_financial_data', 'stock_quotes') }}
),

cleaned_quotes as (
    select
        symbol,
        name,
        price,
        previous_close,
        open,
        high,
        low,
        volume,
        market_cap,
        pe_ratio,
        dividend_yield,
        change,
        change_percent,
        currency,
        exchange,
        quote_type,
        timestamp as quote_timestamp,
        message_timestamp,
        job_id,
        source
    from source_data
    where 
        price is not null 
        and price > 0
        and symbol is not null
),

enriched_quotes as (
    select
        *,
        -- Calculate additional metrics
        case 
            when change_percent > 5 then 'Strong Up'
            when change_percent > 0 then 'Up'
            when change_percent > -5 then 'Sideways'
            when change_percent > -10 then 'Down'
            else 'Strong Down'
        end as price_trend,
        
        case 
            when market_cap >= 200000000000 then 'Mega Cap'
            when market_cap >= 10000000000 then 'Large Cap'
            when market_cap >= 2000000000 then 'Mid Cap'
            when market_cap >= 300000000 then 'Small Cap'
            else 'Micro Cap'
        end as market_cap_category,
        
        -- Calculate price volatility (simplified)
        abs(change_percent) as volatility,
        
        -- Add data quality flags
        case 
            when price is null or price <= 0 then false
            when volume is null or volume < 0 then false
            else true
        end as is_valid_quote
        
    from cleaned_quotes
)

select * from enriched_quotes
