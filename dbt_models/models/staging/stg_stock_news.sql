-- Staging model for stock news
-- Cleans and standardizes stock news data

with source_data as (
    select * from {{ source('raw_financial_data', 'stock_news') }}
),

cleaned_news as (
    select
        symbol,
        title,
        url,
        text,
        source as news_source,
        news_type,
        published_time,
        image_url,
        message_timestamp,
        job_id,
        source as data_source
    from source_data
    where 
        symbol is not null
        and title is not null
),

enriched_news as (
    select
        *,
        -- Calculate word count
        array_length(string_to_array(title, ' '), 1) + 
        array_length(string_to_array(coalesce(text, ''), ' '), 1) as word_count,
        
        -- Categorize news
        case 
            when lower(title) like '%earnings%' or lower(title) like '%revenue%' or lower(title) like '%profit%' then 'Financial'
            when lower(title) like '%merger%' or lower(title) like '%acquisition%' or lower(title) like '%deal%' then 'M&A'
            when lower(title) like '%product%' or lower(title) like '%launch%' or lower(title) like '%release%' then 'Product'
            when lower(title) like '%ceo%' or lower(title) like '%executive%' or lower(title) like '%management%' then 'Leadership'
            else 'General'
        end as news_category,
        
        -- Simple sentiment analysis (basic keyword matching)
        case 
            when lower(title) like '%positive%' or lower(title) like '%growth%' or lower(title) like '%gain%' then 1
            when lower(title) like '%negative%' or lower(title) like '%decline%' or lower(title) like '%loss%' then -1
            else 0
        end as sentiment_score,
        
        -- Add data quality flags
        case 
            when length(title) < 10 then false
            when url is null then false
            else true
        end as is_valid_news
        
    from cleaned_news
)

select * from enriched_news
