-- Fact table for stock quotes
-- Creates a comprehensive fact table with all stock quote metrics

with stock_quotes as (
    select * from {{ ref('stg_stock_quotes') }}
    where is_valid_quote = true
),

dim_stocks as (
    select * from {{ ref('dim_stocks') }}
),

-- Join quotes with stock dimension
quotes_with_dimensions as (
    select
        sq.*,
        ds.stock_id,
        ds.sector,
        ds.industry,
        ds.market_cap_category
    from stock_quotes sq
    left join dim_stocks ds on sq.symbol = ds.symbol
),

-- Add time dimensions
quotes_with_time as (
    select
        *,
        date(quote_timestamp) as quote_date,
        extract(hour from quote_timestamp) as quote_hour,
        extract(dow from quote_timestamp) as day_of_week,
        case 
            when extract(dow from quote_timestamp) in (0, 6) then 'Weekend'
            else 'Weekday'
        end as day_type
    from quotes_with_dimensions
),

-- Add calculated metrics
enriched_quotes as (
    select
        -- Primary key
        {{ dbt_utils.generate_surrogate_key(['symbol', 'quote_timestamp']) }} as quote_id,
        
        -- Foreign keys
        stock_id,
        symbol,
        
        -- Time dimensions
        quote_timestamp,
        quote_date,
        quote_hour,
        day_of_week,
        day_type,
        
        -- Price metrics
        price,
        previous_close,
        open,
        high,
        low,
        change,
        change_percent,
        
        -- Volume metrics
        volume,
        case 
            when previous_close > 0 then (price - previous_close) / previous_close * volume
            else 0
        end as dollar_volume,
        
        -- Market metrics
        market_cap,
        pe_ratio,
        dividend_yield,
        
        -- Derived metrics
        price_trend,
        volatility,
        market_cap_category,
        
        -- Business dimensions
        sector,
        industry,
        
        -- Data quality
        is_valid_quote,
        
        -- Metadata
        message_timestamp,
        job_id,
        source as data_source,
        
        current_timestamp as created_at
        
    from quotes_with_time
)

select * from enriched_quotes
