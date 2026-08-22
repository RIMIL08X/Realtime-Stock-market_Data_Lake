import os
import time
import requests
import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------
# API Configuration
# ---------------------------------------------------------
API_BASE_URL = os.getenv("API_BASE_URL", "https://financial-api-mwp5.onrender.com").strip()

if API_BASE_URL.lower() == "financial-api":
    API_BASE_URL = "https://financial-api-mwp5.onrender.com"
elif not API_BASE_URL.startswith("http://") and not API_BASE_URL.startswith("https://"):
    if "onrender.com" in API_BASE_URL:
        API_BASE_URL = f"https://{API_BASE_URL}"
    else:
        API_BASE_URL = f"http://{API_BASE_URL}:8000"

# ---------------------------------------------------------
# Streamlit Page Setup
# ---------------------------------------------------------
st.set_page_config(
    page_title="Real-Time Stock Market Data Lake | Production Stream Engine",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ---------------------------------------------------------
# Ultra-Flashy Modern Cyberpunk Glassmorphic Design Token System
# ---------------------------------------------------------
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:ital,wght@0,400;0,700;1,400&family=Outfit:wght@400;600;700;800;900&display=swap');

    html, body, [class*="css"] {
        font-family: 'Outfit', -apple-system, BlinkMacSystemFont, sans-serif;
    }
    
    .stApp {
        background: radial-gradient(ellipse at 50% 0%, #131722 0%, #080A0F 100%);
        color: #F1F5F9;
    }

    /* Pulsing LED Indicator */
    @keyframes pulse-green {
        0% { box-shadow: 0 0 0 0 rgba(0, 245, 212, 0.7); }
        70% { box-shadow: 0 0 0 10px rgba(0, 245, 212, 0); }
        100% { box-shadow: 0 0 0 0 rgba(0, 245, 212, 0); }
    }

    .live-dot {
        width: 10px;
        height: 10px;
        background-color: #00F5D4;
        border-radius: 50%;
        display: inline-block;
        margin-right: 8px;
        animation: pulse-green 2s infinite;
    }

    /* Top Hero Header */
    .hero-banner {
        background: linear-gradient(135deg, rgba(26, 31, 46, 0.85) 0%, rgba(13, 17, 26, 0.95) 100%);
        border: 1px solid rgba(0, 245, 212, 0.25);
        backdrop-filter: blur(24px);
        border-radius: 20px;
        padding: 24px 32px;
        margin-bottom: 28px;
        box-shadow: 0 20px 50px rgba(0, 0, 0, 0.6), inset 0 1px 0 rgba(255, 255, 255, 0.15);
    }

    .hero-header-top {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 20px;
        flex-wrap: wrap;
    }

    .hero-title {
        font-size: 28px;
        font-weight: 900;
        background: linear-gradient(135deg, #00F5D4 0%, #00F0FF 45%, #A855F7 85%, #FF2A6D 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin: 0;
        letter-spacing: -0.5px;
        white-space: nowrap;
        text-shadow: 0 0 30px rgba(0, 245, 212, 0.25);
    }

    .hero-actions {
        display: flex;
        align-items: center;
        gap: 12px;
        flex-wrap: wrap;
    }

    .hero-btn-git {
        background: rgba(255, 255, 255, 0.08);
        color: #F1F5F9;
        text-decoration: none;
        padding: 8px 16px;
        border-radius: 20px;
        font-weight: 600;
        border: 1px solid rgba(255, 255, 255, 0.2);
        font-size: 13px;
        transition: all 0.2s ease;
    }
    .hero-btn-git:hover {
        background: rgba(255, 255, 255, 0.15);
        border-color: rgba(255, 255, 255, 0.4);
    }

    .hero-btn-docs {
        background: rgba(123, 44, 191, 0.3);
        color: #E0A9FF;
        text-decoration: none;
        padding: 8px 16px;
        border-radius: 20px;
        font-weight: 700;
        border: 1px solid rgba(123, 44, 191, 0.5);
        font-size: 13px;
        box-shadow: 0 0 15px rgba(123, 44, 191, 0.3);
        transition: all 0.2s ease;
    }
    .hero-btn-docs:hover {
        background: rgba(123, 44, 191, 0.5);
        box-shadow: 0 0 25px rgba(123, 44, 191, 0.5);
    }

    .hero-btn-live {
        background: rgba(0, 245, 212, 0.15);
        color: #00F5D4;
        padding: 8px 16px;
        border-radius: 20px;
        font-weight: 800;
        border: 1px solid rgba(0, 245, 212, 0.4);
        font-size: 13px;
        display: flex;
        align-items: center;
        white-space: nowrap;
    }

    .hero-subtitle {
        color: #94A3B8;
        font-size: 13px;
        margin-top: 14px;
        font-weight: 500;
        display: flex;
        align-items: center;
        gap: 10px;
        flex-wrap: wrap;
    }

    .tech-pill {
        background: rgba(255, 255, 255, 0.05);
        border: 1px solid rgba(255, 255, 255, 0.1);
        padding: 4px 12px;
        border-radius: 12px;
        font-size: 12px;
        color: #CBD5E1;
        font-family: 'JetBrains Mono', monospace;
    }

    /* Flashy Live Ticker Cards */
    .ticker-card {
        flex: 1;
        background: rgba(21, 26, 38, 0.65);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 16px;
        padding: 16px 20px;
        backdrop-filter: blur(16px);
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        box-shadow: 0 10px 25px rgba(0,0,0,0.3);
        position: relative;
        overflow: hidden;
    }

    .ticker-card::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 2px;
        background: linear-gradient(90deg, #00F5D4, #7B2CBF);
        opacity: 0.5;
        transition: opacity 0.3s ease;
    }

    .ticker-card:hover {
        transform: translateY(-4px) scale(1.02);
        border-color: rgba(0, 245, 212, 0.5);
        box-shadow: 0 16px 32px rgba(0, 245, 212, 0.15);
    }

    .ticker-card:hover::before {
        opacity: 1;
    }

    .ticker-symbol {
        font-size: 13px;
        font-weight: 800;
        color: #94A3B8;
        text-transform: uppercase;
        letter-spacing: 1.5px;
        display: flex;
        justify-content: space-between;
        align-items: center;
    }

    .ticker-price {
        font-family: 'JetBrains Mono', monospace;
        font-size: 24px;
        font-weight: 800;
        color: #FFFFFF;
        margin: 6px 0;
    }

    .ticker-badge-pos {
        color: #00E676;
        font-size: 13px;
        font-weight: 700;
        background: rgba(0, 230, 118, 0.15);
        border: 1px solid rgba(0, 230, 118, 0.3);
        padding: 3px 10px;
        border-radius: 20px;
        display: inline-block;
    }

    .ticker-badge-neg {
        color: #FF5252;
        font-size: 13px;
        font-weight: 700;
        background: rgba(255, 82, 82, 0.15);
        border: 1px solid rgba(255, 82, 82, 0.3);
        padding: 3px 10px;
        border-radius: 20px;
        display: inline-block;
    }

    /* KPI Glass Metric Cards Override */
    div[data-testid="stMetric"] {
        background: rgba(21, 26, 38, 0.75) !important;
        border: 1px solid rgba(0, 240, 255, 0.15) !important;
        backdrop-filter: blur(16px) !important;
        border-radius: 16px !important;
        padding: 18px 22px !important;
        box-shadow: 0 12px 28px rgba(0, 0, 0, 0.4) !important;
        transition: all 0.3s ease !important;
    }

    div[data-testid="stMetric"]:hover {
        border-color: rgba(0, 245, 212, 0.5) !important;
        box-shadow: 0 16px 35px rgba(0, 245, 212, 0.15) !important;
    }

    div[data-testid="stMetricLabel"] {
        color: #94A3B8 !important;
        font-size: 13px !important;
        font-weight: 700 !important;
        text-transform: uppercase !important;
        letter-spacing: 0.8px !important;
    }

    div[data-testid="stMetricValue"] {
        font-family: 'JetBrains Mono', monospace !important;
        color: #00F5D4 !important;
        font-size: 30px !important;
        font-weight: 900 !important;
    }

    /* Sidebar Styling */
    section[data-testid="stSidebar"] {
        background-color: #0B0E14 !important;
        border-right: 1px solid rgba(255, 255, 255, 0.08) !important;
    }

    div[role="radiogroup"] > label {
        background: rgba(255, 255, 255, 0.03) !important;
        border: 1px solid rgba(255, 255, 255, 0.08) !important;
        padding: 12px 18px !important;
        border-radius: 12px !important;
        margin-bottom: 10px !important;
        transition: all 0.2s ease !important;
        font-weight: 600 !important;
    }

    div[role="radiogroup"] > label:hover {
        background: rgba(0, 245, 212, 0.12) !important;
        border-color: rgba(0, 245, 212, 0.4) !important;
    }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------
# High-Reliability Data Fetcher with Retry & Cache
# ---------------------------------------------------------
@st.cache_data(ttl=5)
def fetch_api_data(endpoint: str, symbol: str, limit: int = 500):
    url = f"{API_BASE_URL}/{endpoint}/{symbol}?limit={limit}"
    for _ in range(3):
        try:
            res = requests.get(url, timeout=15)
            if res.status_code == 200:
                data = res.json()
                if data:
                    return pd.DataFrame(data)
        except Exception:
            time.sleep(1)
    return pd.DataFrame()

# Auto wake-up ping to trigger live Yahoo Finance ingestion on dashboard load
try:
    requests.get(f"{API_BASE_URL}/wake", timeout=4)
except Exception:
    pass

# ---------------------------------------------------------
# Hero Banner Header
# ---------------------------------------------------------
api_docs_url = f"{API_BASE_URL}/docs" if API_BASE_URL.startswith("http") else "http://localhost:8000/docs"
github_repo_url = "https://github.com/RIMIL08X/Realtime-Stock-market_Data_Lake"

st.markdown(f"""
<div class="hero-banner">
    <div class="hero-header-top">
        <h1 class="hero-title">⚡ REAL-TIME FINANCIAL DATA LAKE</h1>
        <div class="hero-actions">
            <a href="{github_repo_url}" target="_blank" class="hero-btn-git">
                💻 GitHub Code
            </a>
            <a href="{api_docs_url}" target="_blank" class="hero-btn-docs">
                🔗 REST API Docs
            </a>
            <span class="hero-btn-live">
                <span class="live-dot"></span> LIVE YAHOO STREAM
            </span>
        </div>
    </div>
    <div class="hero-subtitle">
        <span class="tech-pill">Apache Kafka</span>
        <span class="tech-pill">PySpark Streaming</span>
        <span class="tech-pill">Medallion Lakehouse</span>
        <span class="tech-pill">Yahoo Finance Engine</span>
    </div>
</div>
""", unsafe_allow_html=True)

# ---------------------------------------------------------
# Sidebar Controls & System Health Matrix
# ---------------------------------------------------------
st.sidebar.markdown("### 🎛️ CONTROL CENTER")
selected_symbol = st.sidebar.selectbox("Select Asset Ticker", ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"], index=0)

view_tab = st.sidebar.radio(
    "Analytics Suite",
    [
        "📊 Live Market OHLC Ticks",
        "📈 Daily Returns & Distribution",
        "📉 Technical Trend & Moving Averages",
        "🌊 Volatility & Market Dynamics",
        "🛡️ Portfolio Risk & Sharpe Ratio"
    ]
)

st.sidebar.markdown("---")
st.sidebar.markdown("### 🔌 PLATFORM HEALTH MATRIX")

try:
    health_resp = requests.get(f"{API_BASE_URL}/health", timeout=10)
    if health_resp.status_code == 200 and health_resp.json().get("status") == "healthy":
        h_data = health_resp.json()
        st.sidebar.markdown(f"""
        <div style="background: rgba(0, 230, 118, 0.12); border: 1px solid rgba(0, 230, 118, 0.4); padding: 14px; border-radius: 12px; color: #00E676; font-size: 13px; font-weight: 700; box-shadow: 0 0 20px rgba(0, 230, 118, 0.15);">
            ✅ REST API & DB CONNECTED<br>
            <span style="font-size: 11px; color: #94A3B8; font-weight: 500;">Host: {h_data.get('connected_host', 'Neon Cloud')}</span>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.sidebar.warning("API Online, Database initializing...")
except Exception:
    st.sidebar.error("REST Serving API Offline")

st.sidebar.markdown("---")
st.sidebar.caption("⚡ Engine v2.0 · Yahoo Finance & Apache Spark 3.5.3")

# ---------------------------------------------------------
# Top 5 Multi-Asset Flashy Ticker Cards
# ---------------------------------------------------------
ticker_cols = st.columns(5)
all_symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]

for idx, sym in enumerate(all_symbols):
    df_sym = fetch_api_data("ticks", sym, limit=2)
    with ticker_cols[idx]:
        if not df_sym.empty:
            latest_price = df_sym.iloc[0]["close"]
            prev_price = df_sym.iloc[1]["close"] if len(df_sym) > 1 else latest_price
            change = latest_price - prev_price
            change_pct = (change / prev_price) * 100 if prev_price > 0 else 0.0
            
            badge_class = "ticker-badge-pos" if change >= 0 else "ticker-badge-neg"
            sign = "+" if change >= 0 else ""
            
            st.markdown(f"""
            <div class="ticker-card">
                <div class="ticker-symbol">
                    <span>{sym}</span>
                    <span style="font-size: 10px; color: #00F5D4;">● LIVE</span>
                </div>
                <div class="ticker-price">${latest_price:.2f}</div>
                <div class="{badge_class}">{sign}{change_pct:.2f}%</div>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div class="ticker-card">
                <div class="ticker-symbol">{sym}</div>
                <div class="ticker-price">--.--</div>
                <div class="ticker-badge-pos">STANDBY</div>
            </div>
            """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ---------------------------------------------------------
# TAB 1: Live Market OHLC Ticks
# ---------------------------------------------------------
if view_tab == "📊 Live Market OHLC Ticks":
    st.subheader(f"📊 Real-Time Candlestick & Volume Stream — {selected_symbol}")
    df_ticks = fetch_api_data("ticks", selected_symbol, limit=500)

    if not df_ticks.empty:
        df_ticks["timestamp"] = pd.to_datetime(df_ticks["timestamp"])
        df_ticks = df_ticks.sort_values("timestamp")

        latest = df_ticks.iloc[-1]
        first = df_ticks.iloc[0]
        price_change = latest['close'] - first['close']
        pct_change = (price_change / first['close']) * 100 if first['close'] > 0 else 0

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Latest Close", f"${latest['close']:.2f}", f"{price_change:+.2f} ({pct_change:+.2f}%)")
        c2.metric("Session High", f"${df_ticks['high'].max():.2f}")
        c3.metric("Session Low", f"${df_ticks['low'].min():.2f}")
        c4.metric("Tick Volume", f"{int(latest['volume']):,}")
        c5.metric("Data Quality Check", latest["quality_flag"].upper())

        # Subplot Candlestick + Volume
        fig = make_subplots(
            rows=2, cols=1, shared_xaxes=True,
            vertical_spacing=0.04, subplot_titles=(f"{selected_symbol} Real-Time Price Action (Candlesticks)", "Stream Volume"),
            row_width=[0.25, 0.75]
        )

        fig.add_trace(go.Candlestick(
            x=df_ticks['timestamp'],
            open=df_ticks['open'],
            high=df_ticks['high'],
            low=df_ticks['low'],
            close=df_ticks['close'],
            name="OHLC",
            increasing_line_color='#00F5D4',
            decreasing_line_color='#FF2A6D',
            increasing_fillcolor='rgba(0, 245, 212, 0.35)',
            decreasing_fillcolor='rgba(255, 42, 109, 0.35)'
        ), row=1, col=1)

        colors = ['#00F5D4' if c >= o else '#FF2A6D' for c, o in zip(df_ticks['close'], df_ticks['open'])]
        fig.add_trace(go.Bar(
            x=df_ticks['timestamp'],
            y=df_ticks['volume'],
            name="Volume",
            marker_color=colors,
            opacity=0.75
        ), row=2, col=1)

        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(15, 23, 42, 0.65)',
            height=580,
            showlegend=False,
            margin=dict(l=20, r=20, t=40, b=20),
            xaxis_rangeslider_visible=False
        )

        fig.update_xaxes(showgrid=True, gridcolor='rgba(255,255,255,0.06)')
        fig.update_yaxes(showgrid=True, gridcolor='rgba(255,255,255,0.06)')

        st.plotly_chart(fig, use_container_width=True)

        with st.expander("🔍 Inspect Raw Silver Stream Records (PostgreSQL Database Table)"):
            col_exp1, col_exp2 = st.columns([3, 1])
            with col_exp2:
                if st.button("🔄 Refresh Live Stream Ticks", key="btn_refresh_ticks", use_container_width=True):
                    try:
                        requests.get(f"{API_BASE_URL}/wake", timeout=10)
                        st.cache_data.clear()
                        st.success("⚡ Live Yahoo Finance ingestion triggered! Fetching fresh records...")
                        time.sleep(1.5)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error triggering wake-up ingestion: {e}")
            
            st.dataframe(df_ticks.sort_values("timestamp", ascending=False), use_container_width=True)
    else:
        st.info(f"Awaiting streaming data for {selected_symbol}...")

# ---------------------------------------------------------
# TAB 2: Daily Returns & Distribution
# ---------------------------------------------------------
elif view_tab == "📈 Daily Returns & Distribution":
    st.subheader(f"📈 Returns Analysis & Statistical Distribution — {selected_symbol}")
    df_ret = fetch_api_data("returns", selected_symbol, limit=500)

    if not df_ret.empty:
        df_ret["trade_date"] = pd.to_datetime(df_ret["trade_date"])
        df_ret = df_ret.sort_values("trade_date")

        valid_ret = df_ret.dropna(subset=["daily_return"])
        if not valid_ret.empty:
            mean_ret = valid_ret["daily_return"].mean() * 100
            std_ret = valid_ret["daily_return"].std() * 100
            cum_ret = ((1 + valid_ret["daily_return"]).prod() - 1) * 100
            skew = valid_ret["daily_return"].skew()

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Cumulative Return", f"{cum_ret:+.2f}%")
            c2.metric("Mean Daily Return", f"{mean_ret:+.3f}%")
            c3.metric("Return Volatility (StdDev)", f"{std_ret:.3f}%")
            c4.metric("Distribution Skewness", f"{skew:+.2f}")

            col_left, col_right = st.columns([1.2, 1])

            with col_left:
                fig_bar = px.bar(
                    valid_ret, x="trade_date", y="daily_return",
                    title=f"{selected_symbol} Daily Returns Timeline",
                    template="plotly_dark"
                )
                fig_bar.update_traces(marker_color=np.where(valid_ret['daily_return'] >= 0, '#00F5D4', '#FF2A6D'))
                fig_bar.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(15, 23, 42, 0.65)',
                    height=440, yaxis_tickformat='.2%'
                )
                st.plotly_chart(fig_bar, use_container_width=True)

            with col_right:
                fig_hist = px.histogram(
                    valid_ret, x="daily_return", nbins=30,
                    title=f"{selected_symbol} Frequency Distribution & Tail Risk",
                    template="plotly_dark",
                    color_discrete_sequence=['#00F0FF'],
                    marginal="box"
                )
                fig_hist.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(15, 23, 42, 0.65)',
                    height=440, xaxis_tickformat='.2%'
                )
                st.plotly_chart(fig_hist, use_container_width=True)
        else:
            st.warning("Insufficient return observations for distribution analysis.")
    else:
        st.info(f"No daily return data available for {selected_symbol}.")

# ---------------------------------------------------------
# TAB 3: Technical Trend & Moving Averages
# ---------------------------------------------------------
elif view_tab == "📉 Technical Trend & Moving Averages":
    st.subheader(f"📉 Moving Average Overlay & Trend Signals — {selected_symbol}")
    df_ma = fetch_api_data("ma", selected_symbol, limit=500)
    df_ticks = fetch_api_data("ticks", selected_symbol, limit=500)

    if not df_ma.empty:
        df_ma["trade_date"] = pd.to_datetime(df_ma["trade_date"])
        df_ma = df_ma.sort_values("trade_date")

        latest_ma20 = df_ma.iloc[-1]["ma_20"]
        latest_ma50 = df_ma.iloc[-1]["ma_50"]
        latest_ma200 = df_ma.iloc[-1]["ma_200"]

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("MA 20 (Short)", f"${latest_ma20:.2f}" if latest_ma20 else "N/A")
        c2.metric("MA 50 (Medium)", f"${latest_ma50:.2f}" if latest_ma50 else "N/A")
        c3.metric("MA 200 (Long)", f"${latest_ma200:.2f}" if latest_ma200 else "N/A")
        
        signal = "BULLISH 🚀" if latest_ma20 and latest_ma50 and latest_ma20 > latest_ma50 else "BEARISH 📉"
        c4.metric("Trend Signal", signal)

        fig = go.Figure()

        if not df_ticks.empty:
            df_ticks["trade_date"] = pd.to_datetime(df_ticks["timestamp"]).dt.date
            df_daily = df_ticks.groupby("trade_date")["close"].last().reset_index()
            df_daily["trade_date"] = pd.to_datetime(df_daily["trade_date"])
            fig.add_trace(go.Scatter(
                x=df_daily["trade_date"], y=df_daily["close"],
                mode="lines", name="Price", line=dict(color="#F8FAFC", width=3)
            ))

        fig.add_trace(go.Scatter(
            x=df_ma["trade_date"], y=df_ma["ma_20"],
            mode="lines", name="MA 20 (Short)", line=dict(color="#00F0FF", width=2.5)
        ))
        fig.add_trace(go.Scatter(
            x=df_ma["trade_date"], y=df_ma["ma_50"],
            mode="lines", name="MA 50 (Medium)", line=dict(color="#7B2CBF", width=2.5)
        ))
        fig.add_trace(go.Scatter(
            x=df_ma["trade_date"], y=df_ma["ma_200"],
            mode="lines", name="MA 200 (Long)", line=dict(color="#FFB703", width=2.5)
        ))

        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(15, 23, 42, 0.65)',
            title=f"{selected_symbol} Price vs Moving Averages (MA20 / MA50 / MA200)",
            xaxis_title="Date",
            yaxis_title="Price ($)",
            height=500
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Moving average metrics pending execution.")

# ---------------------------------------------------------
# TAB 4: Volatility & Market Dynamics
# ---------------------------------------------------------
elif view_tab == "🌊 Volatility & Market Dynamics":
    st.subheader(f"🌊 Rolling & Annualized Volatility Dynamics — {selected_symbol}")
    df_vol = fetch_api_data("volatility", selected_symbol, limit=500)

    if not df_vol.empty:
        df_vol["trade_date"] = pd.to_datetime(df_vol["trade_date"])
        df_vol = df_vol.sort_values("trade_date")

        valid_vol = df_vol.dropna(subset=["rolling_volatility"])
        if not valid_vol.empty:
            curr_vol = valid_vol.iloc[-1]["annualized_volatility"] * 100
            avg_vol = valid_vol["annualized_volatility"].mean() * 100

            c1, c2 = st.columns(2)
            c1.metric("Current Annualized Volatility", f"{curr_vol:.2f}%")
            c2.metric("20-Day Mean Volatility", f"{avg_vol:.2f}%")

            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=valid_vol["trade_date"], y=valid_vol["rolling_volatility"],
                mode="lines+markers", name="20-Day Rolling Volatility",
                line=dict(color="#00F5D4", width=3),
                fill='tozeroy', fillcolor='rgba(0, 245, 212, 0.15)'
            ))
            fig.add_trace(go.Scatter(
                x=valid_vol["trade_date"], y=valid_vol["annualized_volatility"],
                mode="lines+markers", name="Annualized Volatility (x√252)",
                line=dict(color="#FFB703", width=3)
            ))

            fig.update_layout(
                template="plotly_dark",
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(15, 23, 42, 0.65)',
                title=f"{selected_symbol} Volatility Expansion & Contraction",
                height=480,
                yaxis_tickformat='.2%'
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Volatility window requires minimum observations.")
    else:
        st.info("Volatility metrics table is empty.")

# ---------------------------------------------------------
# TAB 5: Portfolio Risk & Sharpe Ratio
# ---------------------------------------------------------
elif view_tab == "🛡️ Portfolio Risk & Sharpe Ratio":
    st.subheader(f"🛡️ Portfolio Risk Matrix & Sharpe Efficiency — {selected_symbol}")
    df_sharpe = fetch_api_data("sharpe", selected_symbol, limit=500)
    df_risk = fetch_api_data("risk", selected_symbol, limit=500)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### ⚡ Sharpe Ratio (Risk-Free Rate = 4.5%)")
        if not df_sharpe.empty:
            df_sharpe["trade_date"] = pd.to_datetime(df_sharpe["trade_date"])
            valid_s = df_sharpe.dropna(subset=["sharpe_ratio"])
            if not valid_s.empty:
                latest_sr = valid_s.iloc[-1]["sharpe_ratio"]
                st.metric("Latest Sharpe Ratio", f"{latest_sr:.2f}")

                fig_s = px.area(
                    valid_s, x="trade_date", y="sharpe_ratio",
                    title=f"{selected_symbol} Sharpe Ratio Timeline",
                    template="plotly_dark",
                    color_discrete_sequence=['#7B2CBF']
                )
                fig_s.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(15, 23, 42, 0.65)', height=420)
                st.plotly_chart(fig_s, use_container_width=True)
            else:
                st.info("Sharpe calculation requires >= 20 observations.")
        else:
            st.info("Sharpe table is empty.")

    with col2:
        st.markdown("#### 📉 Value at Risk (VaR 95%) & CVaR Tail Exposure")
        if not df_risk.empty:
            df_risk["trade_date"] = pd.to_datetime(df_risk["trade_date"])
            valid_r = df_risk.dropna(subset=["var_95"])
            if not valid_r.empty:
                latest_var = valid_r.iloc[-1]["var_95"] * 100
                latest_cvar = valid_r.iloc[-1]["cvar_95"] * 100

                m1, m2 = st.columns(2)
                m1.metric("VaR (95%)", f"{latest_var:.2f}%")
                m2.metric("CVaR (95% Expected Shortfall)", f"{latest_cvar:.2f}%")

                fig_r = go.Figure()
                fig_r.add_trace(go.Scatter(
                    x=valid_r["trade_date"], y=valid_r["var_95"],
                    mode="lines+markers", name="VaR 95%", line=dict(color="#FF2A6D", width=2.5)
                ))
                fig_r.add_trace(go.Scatter(
                    x=valid_r["trade_date"], y=valid_r["cvar_95"],
                    mode="lines+markers", name="CVaR 95%", line=dict(color="#7000FF", width=2.5)
                ))
                fig_r.update_layout(
                    template="plotly_dark",
                    paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(15, 23, 42, 0.65)',
                    title=f"{selected_symbol} Tail Risk Exposure (VaR vs CVaR)",
                    height=420, yaxis_tickformat='.2%'
                )
                st.plotly_chart(fig_r, use_container_width=True)
            else:
                st.info("Risk calculations require >= 20 observations.")
        else:
            st.info("Risk table is empty.")
