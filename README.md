# Real-Time Stock Market Data Lake & Analytics Platform

[![Live Streamlit Dashboard](https://img.shields.io/badge/Live_Dashboard-Streamlit-FF4B4B?style=for-the-badge&logo=streamlit)](https://streamlit-dashboard-xxxx.onrender.com)
[![FastAPI Docs](https://img.shields.io/badge/REST_API-FastAPI-009688?style=for-the-badge&logo=fastapi)](https://financial-api-xxxx.onrender.com/docs)
[![Apache Spark](https://img.shields.io/badge/Engine-Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark)](https://spark.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Streaming-Apache_Kafka-231F20?style=for-the-badge&logo=apachekafka)](https://kafka.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/Database-PostgreSQL-4169E1?style=for-the-badge&logo=postgresql)](https://neon.tech/)

A production-quality real-time financial data lake built on Apache Kafka, Spark Structured Streaming, and PostgreSQL, demonstrating end-to-end data engineering competency — from raw market data ingestion through validated analytics to a served precision dashboard.

---

## 🚀 Live Demo Links

- 📊 **Interactive Dashboard**: `https://streamlit-dashboard.onrender.com` (Replace with your actual Render Streamlit URL)
- ⚡ **REST API Documentation**: `https://financial-api.onrender.com/docs` (Replace with your actual Render API URL)

---

## 🏛️ Architecture & Medallion Design

```
+-------------------+      +-------------------+      +-------------------------+
| Twelve Data API   | ---> | Apache Kafka      | ---> | Spark Structured        |
| (5 Stock Symbols) |      | (market.raw topic)|      | Streaming Pipeline      |
+-------------------+      +-------------------+      +-------------------------+
                                                                   |
                                                                   v
+---------------------------------------------------------------------------------+
|                         PostgreSQL Medallion Data Lake                          |
|                                                                                 |
|  +---------------------+    +-------------------------+    +-----------------+  |
|  | BRONZE LAYER        | -> | SILVER LAYER            | -> | GOLD LAYER      |  |
|  | bronze.stock_ticks  |    | silver.cleaned_ticks    |    | gold.returns    |  |
|  | Raw JSONB Payloads  |    | Validated & Cleaned     |    | gold.ma         |  |
|  +---------------------+    +-------------------------+    | gold.volatility |  |
|                                                            | gold.sharpe     |  |
|                                                            | gold.risk       |  |
|                                                            +-----------------+  |
+---------------------------------------------------------------------------------+
                                                                   |
                                                                   v
                                                      +--------------------------+
                                                      | FastAPI Serving Layer    |
                                                      | & Streamlit Dashboard    |
                                                      +--------------------------+
```

---

## 🛠️ Financial Metrics Implemented

- **Daily Returns**: \((Close_t - Close_{t-1}) / Close_{t-1}\)
- **Moving Averages**: Short (MA20), Medium (MA50), Long (MA200) trendlines.
- **Volatility Metrics**: 20-day rolling standard deviation & Annualized Volatility (\(Rolling\_Vol \times \sqrt{252}\)).
- **Sharpe Ratio**: Risk-adjusted excess return \((Annualized\_Return - R_f) / Annualized\_Vol\).
- **Portfolio Risk Exposure**: Max Drawdown, Historical 95% Value at Risk (VaR), and Conditional VaR (CVaR / Expected Shortfall).

---

## ⚡ Local Startup Guide

```powershell
# 1. Clone repository
git clone https://github.com/RIMIL08X/Realtime-Stock-market_Data_Lake.git
cd Realtime-Stock-market_Data_Lake

# 2. Launch containerized stack
docker compose up -d

# 3. Open browser
http://localhost:8501
```
