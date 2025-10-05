-- Daily summary mart for reporting
-- Aggregates daily market performance metrics

with fact_quotes as (
    select * from {{ ref('fact_stock_quotes') }}
),

fact_news as (
    select * from {{ ref('stg_stock_news') }}
    where is_valid_news = true
),

-- Daily stock performance
daily_stock_performance as (
    select
        quote_date,
        count(distinct symbol) as total_stocks,
        count(*) as total_quotes,
        avg(price) as avg_price,
        sum(volume) as total_volume,
        sum(dollar_volume) as total_dollar_volume,
        avg(change_percent) as avg_change_percent,
        count(case when change_percent > 0 then 1 end) as gainers,
        count(case when change_percent < 0 then 1 end) as losers,
        count(case when change_percent = 0 then 1 end) as unchanged,
        max(change_percent) as max_gain,
        min(change_percent) as max_loss,
        avg(volatility) as avg_volatility
    from fact_quotes
    group by quote_date
),

-- Daily news summary
daily_news_summary as (
    select
        date(published_time) as news_date,
        count(*) as news_count,
        count(distinct symbol) as stocks_with_news,
        avg(sentiment_score) as avg_sentiment,
        count(case when sentiment_score > 0 then 1 end) as positive_news,
        count(case when sentiment_score < 0 then 1 end) as negative_news,
        count(case when sentiment_score = 0 then 1 end) as neutral_news
    from fact_news
    where published_time is not null
    group by date(published_time)
),

-- Top performers by day
daily_top_performers as (
    select
        quote_date,
        first_value(symbol) over (partition by quote_date order by change_percent desc) as top_gainer,
        first_value(change_percent) over (partition by quote_date order by change_percent desc) as top_gainer_percent,
        first_value(symbol) over (partition by quote_date order by change_percent asc) as top_loser,
        first_value(change_percent) over (partition by quote_date order by change_percent asc) as top_loser_percent,
        first_value(symbol) over (partition by quote_date order by volume desc) as most_active,
        first_value(volume) over (partition by quote_date order by volume desc) as most_active_volume
    from fact_quotes
    qualify row_number() over (partition by quote_date order by change_percent desc) = 1
),

-- Sector performance
daily_sector_performance as (
    select
        quote_date,
        sector,
        count(distinct symbol) as stocks_in_sector,
        avg(change_percent) as sector_avg_change,
        sum(volume) as sector_volume
    from fact_quotes
    where sector is not null
    group by quote_date, sector
),

-- Market sentiment calculation
market_sentiment as (
    select
        quote_date,
        case 
            when gainers > losers * 1.5 then 'Very Bullish'
            when gainers > losers then 'Bullish'
            when losers > gainers * 1.5 then 'Very Bearish'
            when losers > gainers then 'Bearish'
            else 'Neutral'
        end as market_sentiment,
        case 
            when avg_change_percent > 2 then 'Strong Up'
            when avg_change_percent > 0 then 'Up'
            when avg_change_percent > -2 then 'Sideways'
            when avg_change_percent > -5 then 'Down'
            else 'Strong Down'
        end as market_trend
    from daily_stock_performance
),

-- Combine all metrics
final_summary as (
    select
        dsp.quote_date as date,
        dsp.total_stocks,
        dsp.total_quotes,
        dsp.avg_price,
        dsp.total_volume,
        dsp.total_dollar_volume,
        dsp.avg_change_percent,
        dsp.gainers,
        dsp.losers,
        dsp.unchanged,
        dsp.max_gain,
        dsp.max_loss,
        dsp.avg_volatility,
        
        -- News metrics
        coalesce(dns.news_count, 0) as news_count,
        coalesce(dns.stocks_with_news, 0) as stocks_with_news,
        coalesce(dns.avg_sentiment, 0) as avg_sentiment,
        coalesce(dns.positive_news, 0) as positive_news,
        coalesce(dns.negative_news, 0) as negative_news,
        coalesce(dns.neutral_news, 0) as neutral_news,
        
        -- Top performers
        dtp.top_gainer,
        dtp.top_gainer_percent,
        dtp.top_loser,
        dtp.top_loser_percent,
        dtp.most_active,
        dtp.most_active_volume,
        
        -- Market sentiment
        ms.market_sentiment,
        ms.market_trend,
        
        -- Additional calculated metrics
        case 
            when dsp.total_stocks > 0 then dsp.gainers::float / dsp.total_stocks * 100
            else 0
        end as gainer_percentage,
        
        case 
            when dsp.total_stocks > 0 then dsp.losers::float / dsp.total_stocks * 100
            else 0
        end as loser_percentage,
        
        case 
            when dsp.avg_price > 0 then dsp.total_dollar_volume / dsp.avg_price
            else 0
        end as estimated_shares_traded,
        
        current_timestamp as created_at
        
    from daily_stock_performance dsp
    left join daily_news_summary dns on dsp.quote_date = dns.news_date
    left join daily_top_performers dtp on dsp.quote_date = dtp.quote_date
    left join market_sentiment ms on dsp.quote_date = ms.quote_date
)

select * from final_summary
