-- Stock dimension table
-- Creates a comprehensive stock dimension with all relevant attributes

with stock_quotes as (
    select distinct
        symbol,
        name,
        exchange,
        currency
    from {{ ref('stg_stock_quotes') }}
    where is_valid_quote = true
),

market_screeners as (
    select distinct
        symbol,
        market_cap_category
    from {{ ref('stg_market_screeners') }}
    where is_valid_screener = true
),

stock_news as (
    select distinct
        symbol,
        news_category
    from {{ ref('stg_stock_news') }}
    where is_valid_news = true
),

-- Get the most recent data for each stock
latest_quotes as (
    select
        symbol,
        name,
        exchange,
        currency,
        row_number() over (partition by symbol order by quote_timestamp desc) as rn
    from {{ ref('stg_stock_quotes') }}
    where is_valid_quote = true
),

latest_screeners as (
    select
        symbol,
        market_cap_category,
        row_number() over (partition by symbol order by message_timestamp desc) as rn
    from {{ ref('stg_market_screeners') }}
    where is_valid_screener = true
),

latest_news as (
    select
        symbol,
        news_category,
        row_number() over (partition by symbol order by message_timestamp desc) as rn
    from {{ ref('stg_stock_news') }}
    where is_valid_news = true
),

-- Combine all data sources
combined_stocks as (
    select
        coalesce(q.symbol, s.symbol, n.symbol) as symbol,
        coalesce(q.name, 'Unknown') as name,
        coalesce(q.exchange, 'Unknown') as exchange,
        coalesce(q.currency, 'USD') as currency,
        coalesce(s.market_cap_category, 'Unknown') as market_cap_category,
        coalesce(n.news_category, 'General') as primary_news_category
    from latest_quotes q
    full outer join latest_screeners s on q.symbol = s.symbol and s.rn = 1
    full outer join latest_news n on coalesce(q.symbol, s.symbol) = n.symbol and n.rn = 1
    where coalesce(q.rn, s.rn, n.rn) = 1
),

-- Add derived attributes
enriched_stocks as (
    select
        symbol,
        name,
        exchange,
        currency,
        market_cap_category,
        primary_news_category,
        
        -- Generate stock ID
        {{ dbt_utils.generate_surrogate_key(['symbol']) }} as stock_id,
        
        -- Determine if stock is active (has recent data)
        case 
            when symbol in (select distinct symbol from {{ ref('stg_stock_quotes') }} where quote_timestamp >= current_date - interval '7 days') then true
            else false
        end as is_active,
        
        -- Add sector classification (simplified)
        case 
            when lower(name) like '%technology%' or lower(name) like '%tech%' then 'Technology'
            when lower(name) like '%health%' or lower(name) like '%medical%' then 'Healthcare'
            when lower(name) like '%financial%' or lower(name) like '%bank%' then 'Financial Services'
            when lower(name) like '%energy%' or lower(name) like '%oil%' or lower(name) like '%gas%' then 'Energy'
            when lower(name) like '%consumer%' or lower(name) like '%retail%' then 'Consumer Discretionary'
            when lower(name) like '%utility%' then 'Utilities'
            when lower(name) like '%industrial%' then 'Industrials'
            when lower(name) like '%material%' then 'Materials'
            when lower(name) like '%real estate%' or lower(name) like '%reit%' then 'Real Estate'
            else 'Other'
        end as sector,
        
        -- Add industry classification (simplified)
        case 
            when lower(name) like '%software%' then 'Software'
            when lower(name) like '%hardware%' then 'Hardware'
            when lower(name) like '%pharmaceutical%' then 'Pharmaceuticals'
            when lower(name) like '%bank%' then 'Banking'
            when lower(name) like '%insurance%' then 'Insurance'
            when lower(name) like '%oil%' then 'Oil & Gas'
            when lower(name) like '%retail%' then 'Retail'
            when lower(name) like '%automotive%' then 'Automotive'
            else 'Other'
        end as industry,
        
        current_timestamp as created_at,
        current_timestamp as updated_at
        
    from combined_stocks
)

select * from enriched_stocks
