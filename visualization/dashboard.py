"""
Financial Data Dashboard using Streamlit
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime, timedelta
import json
from pathlib import Path

# Page configuration
st.set_page_config(
    page_title="Financial Data Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .positive {
        color: #00ff00;
    }
    .negative {
        color: #ff0000;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data
def load_sample_data():
    """Load sample financial data."""
    # This would typically load from your database or data lake
    # For demo purposes, we'll create sample data
    
    # Sample stock quotes
    dates = pd.date_range(start='2024-01-01', end='2024-01-31', freq='D')
    symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META', 'NFLX']
    
    stock_data = []
    for symbol in symbols:
        for date in dates:
            base_price = np.random.uniform(100, 500)
            change_pct = np.random.normal(0, 2)
            price = base_price * (1 + change_pct / 100)
            volume = np.random.randint(1000000, 10000000)
            
            stock_data.append({
                'symbol': symbol,
                'date': date,
                'price': round(price, 2),
                'change_percent': round(change_pct, 2),
                'volume': volume,
                'market_cap': round(price * volume * 0.1, 0)
            })
    
    return pd.DataFrame(stock_data)

@st.cache_data
def load_news_data():
    """Load sample news data."""
    news_data = []
    symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
    
    for symbol in symbols:
        for i in range(10):
            news_data.append({
                'symbol': symbol,
                'title': f'{symbol} News Article {i+1}',
                'published_time': datetime.now() - timedelta(days=np.random.randint(0, 30)),
                'sentiment_score': np.random.uniform(-1, 1),
                'news_category': np.random.choice(['Financial', 'M&A', 'Product', 'Leadership', 'General'])
            })
    
    return pd.DataFrame(news_data)

def create_price_chart(df, symbols):
    """Create price chart for selected symbols."""
    fig = go.Figure()
    
    for symbol in symbols:
        symbol_data = df[df['symbol'] == symbol].sort_values('date')
        fig.add_trace(go.Scatter(
            x=symbol_data['date'],
            y=symbol_data['price'],
            mode='lines+markers',
            name=symbol,
            line=dict(width=2)
        ))
    
    fig.update_layout(
        title="Stock Price Trends",
        xaxis_title="Date",
        yaxis_title="Price ($)",
        hovermode='x unified',
        height=500
    )
    
    return fig

def create_volume_chart(df, symbols):
    """Create volume chart for selected symbols."""
    fig = go.Figure()
    
    for symbol in symbols:
        symbol_data = df[df['symbol'] == symbol].sort_values('date')
        fig.add_trace(go.Bar(
            x=symbol_data['date'],
            y=symbol_data['volume'],
            name=symbol,
            opacity=0.7
        ))
    
    fig.update_layout(
        title="Trading Volume",
        xaxis_title="Date",
        yaxis_title="Volume",
        barmode='group',
        height=400
    )
    
    return fig

def create_performance_heatmap(df):
    """Create performance heatmap."""
    # Pivot data for heatmap
    pivot_data = df.pivot_table(
        values='change_percent', 
        index='symbol', 
        columns='date', 
        aggfunc='mean'
    )
    
    fig = px.imshow(
        pivot_data,
        title="Daily Performance Heatmap (%)",
        color_continuous_scale="RdYlGn",
        aspect="auto"
    )
    
    fig.update_layout(height=400)
    return fig

def create_sentiment_chart(news_df):
    """Create news sentiment chart."""
    # Group by symbol and calculate average sentiment
    sentiment_data = news_df.groupby('symbol')['sentiment_score'].mean().reset_index()
    
    fig = px.bar(
        sentiment_data,
        x='symbol',
        y='sentiment_score',
        title="Average News Sentiment by Stock",
        color='sentiment_score',
        color_continuous_scale="RdYlGn"
    )
    
    fig.update_layout(height=400)
    return fig

def create_market_summary(df):
    """Create market summary metrics."""
    latest_date = df['date'].max()
    latest_data = df[df['date'] == latest_date]
    
    total_stocks = len(latest_data)
    gainers = len(latest_data[latest_data['change_percent'] > 0])
    losers = len(latest_data[latest_data['change_percent'] < 0])
    avg_change = latest_data['change_percent'].mean()
    total_volume = latest_data['volume'].sum()
    
    return {
        'total_stocks': total_stocks,
        'gainers': gainers,
        'losers': losers,
        'avg_change': avg_change,
        'total_volume': total_volume
    }

def main():
    """Main dashboard function."""
    # Header
    st.markdown('<h1 class="main-header">📊 Financial Data Dashboard</h1>', unsafe_allow_html=True)
    
    # Load data
    df = load_sample_data()
    news_df = load_news_data()
    
    # Sidebar
    st.sidebar.title("Filters")
    
    # Date range filter
    date_range = st.sidebar.date_input(
        "Select Date Range",
        value=(df['date'].min().date(), df['date'].max().date()),
        min_value=df['date'].min().date(),
        max_value=df['date'].max().date()
    )
    
    # Symbol filter
    available_symbols = df['symbol'].unique()
    selected_symbols = st.sidebar.multiselect(
        "Select Stocks",
        options=available_symbols,
        default=available_symbols[:5]
    )
    
    # Filter data
    if selected_symbols:
        filtered_df = df[df['symbol'].isin(selected_symbols)]
    else:
        filtered_df = df
    
    if len(date_range) == 2:
        start_date, end_date = date_range
        filtered_df = filtered_df[
            (filtered_df['date'].dt.date >= start_date) & 
            (filtered_df['date'].dt.date <= end_date)
        ]
    
    # Main content
    if filtered_df.empty:
        st.warning("No data available for the selected filters.")
        return
    
    # Market Summary
    st.header("📈 Market Summary")
    
    summary = create_market_summary(filtered_df)
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Total Stocks", summary['total_stocks'])
    
    with col2:
        st.metric("Gainers", summary['gainers'], delta=f"{summary['gainers']/summary['total_stocks']*100:.1f}%")
    
    with col3:
        st.metric("Losers", summary['losers'], delta=f"{summary['losers']/summary['total_stocks']*100:.1f}%")
    
    with col4:
        st.metric("Avg Change", f"{summary['avg_change']:.2f}%", 
                 delta=f"{summary['avg_change']:.2f}%" if summary['avg_change'] != 0 else None)
    
    with col5:
        st.metric("Total Volume", f"{summary['total_volume']:,}")
    
    # Charts
    st.header("📊 Charts")
    
    # Price chart
    if selected_symbols:
        st.subheader("Price Trends")
        price_chart = create_price_chart(filtered_df, selected_symbols)
        st.plotly_chart(price_chart, use_container_width=True)
        
        # Volume chart
        st.subheader("Trading Volume")
        volume_chart = create_volume_chart(filtered_df, selected_symbols)
        st.plotly_chart(volume_chart, use_container_width=True)
    
    # Performance heatmap
    st.subheader("Performance Heatmap")
    heatmap = create_performance_heatmap(filtered_df)
    st.plotly_chart(heatmap, use_container_width=True)
    
    # News sentiment
    st.subheader("News Sentiment Analysis")
    sentiment_chart = create_sentiment_chart(news_df)
    st.plotly_chart(sentiment_chart, use_container_width=True)
    
    # Data tables
    st.header("📋 Data Tables")
    
    # Latest prices
    st.subheader("Latest Prices")
    latest_prices = filtered_df.groupby('symbol').last().reset_index()
    latest_prices = latest_prices[['symbol', 'price', 'change_percent', 'volume', 'market_cap']]
    latest_prices['change_percent'] = latest_prices['change_percent'].apply(lambda x: f"{x:+.2f}%")
    st.dataframe(latest_prices, use_container_width=True)
    
    # Top performers
    st.subheader("Top Performers (Today)")
    top_performers = latest_prices.nlargest(5, 'change_percent')
    st.dataframe(top_performers, use_container_width=True)
    
    # News table
    st.subheader("Latest News")
    news_table = news_df[['symbol', 'title', 'published_time', 'sentiment_score', 'news_category']].head(10)
    st.dataframe(news_table, use_container_width=True)
    
    # Footer
    st.markdown("---")
    st.markdown("**Data Source:** Yahoo Finance API | **Last Updated:** " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

if __name__ == "__main__":
    main()
