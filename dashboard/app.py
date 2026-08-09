import os
import time
import requests
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000").strip()

if not API_BASE_URL.startswith("http://") and not API_BASE_URL.startswith("https://"):
    if "onrender.com" in API_BASE_URL:
        API_BASE_URL = f"https://{API_BASE_URL}"
    else:
        API_BASE_URL = f"http://{API_BASE_URL}:8000"

# ---------------------------------------------------------
# Page Setup
# ---------------------------------------------------------
st.set_page_config(
    page_title="Quantum Financial Data Lake & Analytics",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ---------------------------------------------------------
# Premium Glassmorphism & Neon Design System
# ---------------------------------------------------------
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;700;900&family=JetBrains+Mono:wght@400;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'Outfit', -apple-system, BlinkMacSystemFont, sans-serif;
    }
    
    .stApp {
        background: radial-gradient(circle at 50% 0%, #151A28 0%, #0A0D14 100%);
        color: #E2E8F0;
    }

    /* Top Banner Header */
    .hero-banner {
        background: linear-gradient(135deg, rgba(30, 41, 59, 0.7) 0%, rgba(15, 23, 42, 0.8) 100%);
        border: 1px solid rgba(255, 255, 255, 0.1);
        backdrop-filter: blur(16px);
        border-radius: 16px;
        padding: 24px 32px;
        margin-bottom: 24px;
        box-shadow: 0 20px 40px rgba(0, 0, 0, 0.4), inset 0 1px 0 rgba(255, 255, 255, 0.1);
        display: flex;
        align-items: center;
        justify-content: space-between;
    }

    .hero-title {
        font-size: 32px;
        font-weight: 900;
        background: linear-gradient(135deg, #00F5D4 0%, #00F0FF 50%, #7B2CBF 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin: 0;
        letter-spacing: -0.5px;
    }

    .hero-subtitle {
        color: #94A3B8;
        font-size: 14px;
        margin-top: 6px;
        font-weight: 400;
    }

    /* Live Ticker Bar */
    .ticker-row {
        display: flex;
        gap: 16px;
        margin-bottom: 24px;
        overflow-x: auto;
        padding-bottom: 4px;
    }

    .ticker-card {
        flex: 1;
        min-width: 170px;
        background: rgba(30, 41, 59, 0.4);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 12px;
        padding: 14px 18px;
        backdrop-filter: blur(10px);
        transition: all 0.3s ease;
        box-shadow: 0 8px 16px rgba(0,0,0,0.2);
    }

    .ticker-card:hover {
        transform: translateY(-3px);
        border-color: rgba(0, 245, 212, 0.4);
        box-shadow: 0 12px 24px rgba(0, 245, 212, 0.15);
    }

    .ticker-symbol {
        font-size: 13px;
        font-weight: 700;
        color: #64748B;
        text-transform: uppercase;
        letter-spacing: 1px;
    }

    .ticker-price {
        font-family: 'JetBrains Mono', monospace;
        font-size: 22px;
        font-weight: 700;
        color: #F8FAFC;
        margin: 4px 0;
    }

    .ticker-change-pos {
        color: #00E676;
        font-size: 13px;
        font-weight: 600;
        display: inline-block;
        background: rgba(0, 230, 118, 0.12);
        padding: 2px 8px;
        border-radius: 20px;
    }

    .ticker-change-neg {
        color: #FF5252;
        font-size: 13px;
        font-weight: 600;
        display: inline-block;
        background: rgba(255, 82, 82, 0.12);
        padding: 2px 8px;
        border-radius: 20px;
    }

    /* KPI Glass Metric Cards */
    div[data-testid="stMetric"] {
        background: rgba(26, 31, 46, 0.6) !important;
        border: 1px solid rgba(255, 255, 255, 0.08) !important;
        backdrop-filter: blur(12px) !important;
        border-radius: 14px !important;
        padding: 16px 20px !important;
        box-shadow: 0 10px 25px rgba(0, 0, 0, 0.3) !important;
        transition: all 0.3s ease !important;
    }

    div[data-testid="stMetric"]:hover {
        border-color: rgba(0, 240, 255, 0.3) !important;
        box-shadow: 0 12px 30px rgba(0, 240, 255, 0.1) !important;
    }

    div[data-testid="stMetricLabel"] {
        color: #94A3B8 !important;
        font-size: 13px !important;
        font-weight: 600 !important;
        text-transform: uppercase !important;
        letter-spacing: 0.5px !important;
    }

    div[data-testid="stMetricValue"] {
        font-family: 'JetBrains Mono', monospace !important;
        color: #00F5D4 !important;
        font-size: 28px !important;
        font-weight: 800 !important;
    }

    /* Custom Sidebar Styling */
    section[data-testid="stSidebar"] {
        background-color: #0B0E14 !important;
        border-right: 1px solid rgba(255, 255, 255, 0.08) !important;
    }

    /* Custom Radio Buttons */
    div[role="radiogroup"] > label {
        background: rgba(255, 255, 255, 0.03) !important;
        border: 1px solid rgba(255, 255, 255, 0.06) !important;
        padding: 10px 16px !important;
        border-radius: 10px !important;
        margin-bottom: 8px !important;
        transition: all 0.2s ease !important;
    }

    div[role="radiogroup"] > label:hover {
        background: rgba(0, 245, 212, 0.1) !important;
        border-color: rgba(0, 245, 212, 0.3) !important;
    }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------
# Data Fetcher with Caching
# ---------------------------------------------------------
@st.cache_data(ttl=5)
def fetch_api_data(endpoint: str, symbol: str, limit: int = 500):
    try:
        res = requests.get(f"{API_BASE_URL}/{endpoint}/{symbol}?limit={limit}", timeout=5)
        if res.status_code == 200:
            data = res.json()
            if data:
                return pd.DataFrame(data)
    except Exception:
        pass
    return pd.DataFrame()

# ---------------------------------------------------------
# Hero Banner
# ---------------------------------------------------------
api_docs_url = f"{API_BASE_URL}/docs" if API_BASE_URL.startswith("http") else "http://localhost:8000/docs"

st.markdown(f"""
<div class="hero-banner">
    <div>
        <h1 class="hero-title">⚡ REAL-TIME FINANCIAL DATA LAKE</h1>
        <div class="hero-subtitle">Apache Kafka · Spark Structured Streaming · Medallion Architecture (Bronze / Silver / Gold)</div>
    </div>
    <div style="text-align: right; display: flex; gap: 10px; align-items: center;">
        <a href="{api_docs_url}" target="_blank" style="background: rgba(123, 44, 191, 0.25); color: #E0A9FF; text-decoration: none; padding: 6px 14px; border-radius: 20px; font-weight: 600; border: 1px solid rgba(123, 44, 191, 0.4); font-size: 13px;">
            🔗 REST API Docs
        </a>
        <span style="background: rgba(0, 245, 212, 0.15); color: #00F5D4; padding: 6px 14px; border-radius: 20px; font-weight: 700; border: 1px solid rgba(0, 245, 212, 0.3); font-size: 13px;">
            ● LIVE STREAMING ACTIVE
        </span>
    </div>
</div>
""", unsafe_allow_html=True)

# ---------------------------------------------------------
# Sidebar Controls & System Status
# ---------------------------------------------------------
st.sidebar.markdown("### 🎛️ CONTROL CENTER")
selected_symbol = st.sidebar.selectbox("Select Asset", ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"], index=0)

view_tab = st.sidebar.radio(
    "Analytics Suite",
    [
        "📊 Live Market OHLC Ticks",
        "📈 Daily Returns & Distribution",
        "📉 Moving Average Overlay",
        "🌊 Volatility Dynamics",
        "🛡️ Portfolio Risk & Sharpe Ratio"
    ]
)

st.sidebar.markdown("---")
st.sidebar.markdown("### 🔌 PLATFORM HEALTH")

try:
    health_resp = requests.get(f"{API_BASE_URL}/health", timeout=10)
    if health_resp.status_code == 200 and health_resp.json().get("status") == "healthy":
        st.sidebar.markdown("""
        <div style="background: rgba(0, 230, 118, 0.1); border: 1px solid rgba(0, 230, 118, 0.3); padding: 12px; border-radius: 10px; color: #00E676; font-size: 13px; font-weight: 600;">
            ✅ REST API & DB Connected
        </div>
        """, unsafe_allow_html=True)
    else:
        st.sidebar.warning("API Online, Database initializing...")
except Exception as e:
    st.sidebar.error("REST Serving API Offline")

st.sidebar.markdown("---")
st.sidebar.caption("⚡ Financial Engine v1.0 · Confluent Kafka & Apache Spark 3.5.3")

# ---------------------------------------------------------
# Top Multi-Asset Ticker Ribbon
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
            
            badge_class = "ticker-change-pos" if change >= 0 else "ticker-change-neg"
            sign = "+" if change >= 0 else ""
            
            st.markdown(f"""
            <div class="ticker-card">
                <div class="ticker-symbol">{sym}</div>
                <div class="ticker-price">${latest_price:.2f}</div>
                <div class="{badge_class}">{sign}{change_pct:.2f}%</div>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div class="ticker-card">
                <div class="ticker-symbol">{sym}</div>
                <div class="ticker-price">--.--</div>
                <div class="ticker-change-pos">STANDBY</div>
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
        c4.metric("Volume", f"{int(latest['volume']):,}")
        c5.metric("Quality Check", latest["quality_flag"].upper())

        # Subplot Candlestick + Volume
        fig = make_subplots(
            rows=2, cols=1, shared_xaxes=True,
            vertical_spacing=0.03, subplot_titles=(f"{selected_symbol} OHLC Price Action", "Tick Volume"),
            row_width=[0.25, 0.75]
        )

        # Candlestick
        fig.add_trace(go.Candlestick(
            x=df_ticks['timestamp'],
            open=df_ticks['open'],
            high=df_ticks['high'],
            low=df_ticks['low'],
            close=df_ticks['close'],
            name="OHLC",
            increasing_line_color='#00F5D4',
            decreasing_line_color='#FF2A6D',
            increasing_fillcolor='rgba(0, 245, 212, 0.3)',
            decreasing_fillcolor='rgba(255, 42, 109, 0.3)'
        ), row=1, col=1)

        # Volume
        colors = ['#00F5D4' if c >= o else '#FF2A6D' for c, o in zip(df_ticks['close'], df_ticks['open'])]
        fig.add_trace(go.Bar(
            x=df_ticks['timestamp'],
            y=df_ticks['volume'],
            name="Volume",
            marker_color=colors,
            opacity=0.7
        ), row=2, col=1)

        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(15, 23, 42, 0.6)',
            height=580,
            showlegend=False,
            margin=dict(l=20, r=20, t=40, b=20),
            xaxis_rangeslider_visible=False
        )

        fig.update_xaxes(showgrid=True, gridcolor='rgba(255,255,255,0.05)')
        fig.update_yaxes(showgrid=True, gridcolor='rgba(255,255,255,0.05)')

        st.plotly_chart(fig, use_container_width=True)

        with st.expander("🔍 View Raw Silver Stream Records"):
            st.dataframe(df_ticks.tail(50), use_container_width=True)
    else:
        st.info(f"Awaiting streaming data for {selected_symbol}...")

# ---------------------------------------------------------
# TAB 2: Daily Returns & Distribution
# ---------------------------------------------------------
elif view_tab == "📈 Daily Returns & Distribution":
    st.subheader(f"📈 Returns Analysis & Distribution Curve — {selected_symbol}")
    df_ret = fetch_api_data("returns", selected_symbol, limit=500)

    if not df_ret.empty:
        df_ret["trade_date"] = pd.to_datetime(df_ret["trade_date"])
        df_ret = df_ret.sort_values("trade_date")

        valid_ret = df_ret.dropna(subset=["daily_return"])
        if not valid_ret.empty:
            mean_ret = valid_ret["daily_return"].mean() * 100
            std_ret = valid_ret["daily_return"].std() * 100
            cum_ret = ((1 + valid_ret["daily_return"]).prod() - 1) * 100

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Cumulative Return", f"{cum_ret:+.2f}%")
            c2.metric("Mean Daily Return", f"{mean_ret:+.3f}%")
            c3.metric("Return StdDev", f"{std_ret:.3f}%")
            c4.metric("Total Observations", len(valid_ret))

            col_left, col_right = st.columns([1.2, 1])

            with col_left:
                fig_bar = px.bar(
                    valid_ret, x="trade_date", y="daily_return",
                    title=f"{selected_symbol} Daily Returns Timeline",
                    template="plotly_dark"
                )
                fig_bar.update_traces(marker_color=np.where(valid_ret['daily_return'] >= 0, '#00F5D4', '#FF2A6D'))
                fig_bar.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(15, 23, 42, 0.6)',
                    height=420, yaxis_tickformat='.2%'
                )
                st.plotly_chart(fig_bar, use_container_width=True)

            with col_right:
                fig_hist = px.histogram(
                    valid_ret, x="daily_return", nbins=25,
                    title=f"{selected_symbol} Return Frequency Distribution",
                    template="plotly_dark",
                    color_discrete_sequence=['#00F0FF'],
                    marginal="rug"
                )
                fig_hist.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(15, 23, 42, 0.6)',
                    height=420, xaxis_tickformat='.2%'
                )
                st.plotly_chart(fig_hist, use_container_width=True)
        else:
            st.warning("Insufficient return observations for distribution analysis.")
    else:
        st.info(f"No daily return data available for {selected_symbol}.")

# ---------------------------------------------------------
# TAB 3: Moving Average Overlay
# ---------------------------------------------------------
elif view_tab == "📉 Moving Average Overlay":
    st.subheader(f"📉 Trend & Moving Average Crossover — {selected_symbol}")
    df_ma = fetch_api_data("ma", selected_symbol, limit=500)
    df_ticks = fetch_api_data("ticks", selected_symbol, limit=500)

    if not df_ma.empty:
        df_ma["trade_date"] = pd.to_datetime(df_ma["trade_date"])
        df_ma = df_ma.sort_values("trade_date")

        fig = go.Figure()

        if not df_ticks.empty:
            df_ticks["trade_date"] = pd.to_datetime(df_ticks["timestamp"]).dt.date
            df_daily = df_ticks.groupby("trade_date")["close"].last().reset_index()
            df_daily["trade_date"] = pd.to_datetime(df_daily["trade_date"])
            fig.add_trace(go.Scatter(
                x=df_daily["trade_date"], y=df_daily["close"],
                mode="lines", name="Price", line=dict(color="#F8FAFC", width=2.5)
            ))

        fig.add_trace(go.Scatter(
            x=df_ma["trade_date"], y=df_ma["ma_20"],
            mode="lines", name="MA 20 (Short)", line=dict(color="#00F0FF", width=2)
        ))
        fig.add_trace(go.Scatter(
            x=df_ma["trade_date"], y=df_ma["ma_50"],
            mode="lines", name="MA 50 (Medium)", line=dict(color="#7B2CBF", width=2)
        ))
        fig.add_trace(go.Scatter(
            x=df_ma["trade_date"], y=df_ma["ma_200"],
            mode="lines", name="MA 200 (Long)", line=dict(color="#FFB703", width=2)
        ))

        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(15, 23, 42, 0.6)',
            title=f"{selected_symbol} Technical Trend Lines (MA20 / MA50 / MA200)",
            xaxis_title="Date",
            yaxis_title="Price ($)",
            height=500
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Moving average metrics pending execution.")

# ---------------------------------------------------------
# TAB 4: Volatility Dynamics
# ---------------------------------------------------------
elif view_tab == "🌊 Volatility Dynamics":
    st.subheader(f"🌊 Rolling & Annualized Volatility Spectrum — {selected_symbol}")
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
                mode="lines+markers", name="20-Day Rolling Vol",
                line=dict(color="#00F5D4", width=2.5),
                fill='tozeroy', fillcolor='rgba(0, 245, 212, 0.1)'
            ))
            fig.add_trace(go.Scatter(
                x=valid_vol["trade_date"], y=valid_vol["annualized_volatility"],
                mode="lines+markers", name="Annualized Vol (x√252)",
                line=dict(color="#FFB703", width=2.5)
            ))

            fig.update_layout(
                template="plotly_dark",
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(15, 23, 42, 0.6)',
                title=f"{selected_symbol} Volatility Expansion & Contraction",
                height=480,
                yaxis_tickformat='.2%'
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Volatility window requires minimum 20 return observations.")
    else:
        st.info("Volatility metrics table is empty.")

# ---------------------------------------------------------
# TAB 5: Portfolio Risk & Sharpe Ratio
# ---------------------------------------------------------
elif view_tab == "🛡️ Portfolio Risk & Sharpe Ratio":
    st.subheader(f"🛡️ Portfolio Risk & Sharpe Ratio Matrix — {selected_symbol}")
    df_sharpe = fetch_api_data("sharpe", selected_symbol, limit=500)
    df_risk = fetch_api_data("risk", selected_symbol, limit=500)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### ⚡ Sharpe Ratio & Risk-Adjusted Return")
        if not df_sharpe.empty:
            df_sharpe["trade_date"] = pd.to_datetime(df_sharpe["trade_date"])
            valid_s = df_sharpe.dropna(subset=["sharpe_ratio"])
            if not valid_s.empty:
                latest_sr = valid_s.iloc[-1]["sharpe_ratio"]
                st.metric("Latest Sharpe Ratio", f"{latest_sr:.2f}")

                fig_s = px.area(
                    valid_s, x="trade_date", y="sharpe_ratio",
                    title=f"{selected_symbol} Sharpe Ratio Timeline (Rf = 4.5%)",
                    template="plotly_dark",
                    color_discrete_sequence=['#7B2CBF']
                )
                fig_s.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(15, 23, 42, 0.6)', height=400)
                st.plotly_chart(fig_s, use_container_width=True)
            else:
                st.info("Sharpe calculation requires >= 20 observations.")
        else:
            st.info("Sharpe table is empty.")

    with col2:
        st.markdown("#### 📉 Value at Risk (95%) & CVaR Tail Risk")
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
                    mode="lines+markers", name="VaR 95%", line=dict(color="#FF2A6D", width=2)
                ))
                fig_r.add_trace(go.Scatter(
                    x=valid_r["trade_date"], y=valid_r["cvar_95"],
                    mode="lines+markers", name="CVaR 95%", line=dict(color="#7000FF", width=2)
                ))
                fig_r.update_layout(
                    template="plotly_dark",
                    paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(15, 23, 42, 0.6)',
                    title=f"{selected_symbol} Tail Risk Exposure (VaR vs CVaR)",
                    height=400, yaxis_tickformat='.2%'
                )
                st.plotly_chart(fig_r, use_container_width=True)
            else:
                st.info("Risk calculations require >= 20 observations.")
        else:
            st.info("Risk table is empty.")
