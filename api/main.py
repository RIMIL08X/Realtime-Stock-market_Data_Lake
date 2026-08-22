import os
import sys
import logging
from typing import List, Optional
from datetime import date, datetime, timezone
from decimal import Decimal
import threading
import time
import requests

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Ensure scripts directory is on sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from scripts.ingest_yfinance_live import run_ingestion_cycle
except ImportError:
    run_ingestion_cycle = None

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger("FinancialDataAPI")

# Global lock to prevent concurrent duplicate ingestion runs
ingestion_lock = threading.Lock()
last_auto_ingest_time = 0

def auto_ingest_if_needed(force: bool = False):
    """Triggers live Yahoo Finance ingestion cycle if database is stale (> 60s) or forced upon wake-up."""
    global last_auto_ingest_time
    now = time.time()
    
    # Rate-limit automatic wake-up ingestion to once every 30 seconds
    if not force and (now - last_auto_ingest_time < 30):
        return

    if run_ingestion_cycle and ingestion_lock.acquire(blocking=False):
        try:
            logger.info("⚡ Render wake-up detected! Executing instant live Yahoo Finance ingestion cycle...")
            run_ingestion_cycle()
            last_auto_ingest_time = time.time()
            logger.info("✅ Wake-up ingestion completed successfully!")
        except Exception as e:
            logger.error(f"Error during wake-up ingestion: {e}")
        finally:
            ingestion_lock.release()

def keep_alive_worker():
    """Background thread to keep Render web service awake and trigger periodic ingestion."""
    logger.info("Starting Render 24/7 Keep-Alive self-ping background worker...")
    time.sleep(15)
    while True:
        try:
            url = "https://financial-api-mwp5.onrender.com/wake"
            requests.get(url, timeout=15)
            logger.info("Keep-alive self-ping & wake-up refresh sent successfully!")
        except Exception as e:
            logger.warning(f"Keep-alive self-ping warning: {e}")
        time.sleep(300)  # Self-ping every 5 minutes

# Start keep-alive daemon thread
threading.Thread(target=keep_alive_worker, daemon=True).start()

app = FastAPI(
    title="Real-Time Financial Data Lake & Analytics Serving API",
    version="1.0.0",
    description="REST serving layer for Bronze, Silver, and Gold financial data metrics."
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db_connection():
    pg_host = os.getenv("POSTGRES_HOST", "ep-still-heart-a6iddbcp-pooler.us-west-2.aws.neon.tech")
    pg_port = int(os.getenv("POSTGRES_PORT", "5432"))
    pg_db = os.getenv("POSTGRES_DB", "neondb")
    pg_user = os.getenv("POSTGRES_USER", "neondb_owner")
    pg_pass = os.getenv("POSTGRES_PASSWORD", "npg_9mbkxBlLq2CQ")
    
    sslmode = os.getenv("POSTGRES_SSLMODE")
    if not sslmode:
        if "neon.tech" in pg_host or "render.com" in pg_host:
            sslmode = "require"
        else:
            sslmode = "prefer"

    try:
        conn = psycopg2.connect(
            host=pg_host,
            port=pg_port,
            dbname=pg_db,
            user=pg_user,
            password=pg_pass,
            sslmode=sslmode,
            cursor_factory=RealDictCursor,
            connect_timeout=10
        )
        return conn
    except Exception as e:
        logger.error(f"Failed connecting to database ({pg_host}:{pg_port}): {e}")
        raise HTTPException(status_code=500, detail=f"Database connection error: {str(e)}")

def clean_dict_rows(rows):
    cleaned = []
    for row in rows:
        item = {}
        for k, v in row.items():
            if isinstance(v, Decimal):
                item[k] = float(v)
            else:
                item[k] = v
        cleaned.append(item)
    return cleaned

# Pydantic Schemas
class TickResponse(BaseModel):
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    quality_flag: str
    processed_at: datetime

class DailyReturnResponse(BaseModel):
    symbol: str
    trade_date: date
    close: float
    prev_close: Optional[float] = None
    daily_return: Optional[float] = None
    calculated_at: datetime

class MovingAverageResponse(BaseModel):
    symbol: str
    trade_date: date
    ma_20: Optional[float] = None
    ma_50: Optional[float] = None
    ma_200: Optional[float] = None
    calculated_at: datetime

class VolatilityResponse(BaseModel):
    symbol: str
    trade_date: date
    rolling_volatility: Optional[float] = None
    annualized_volatility: Optional[float] = None
    calculated_at: datetime

class SharpeResponse(BaseModel):
    symbol: str
    trade_date: date
    annualized_return: Optional[float] = None
    risk_free_rate: float
    sharpe_ratio: Optional[float] = None
    calculated_at: datetime

class RiskResponse(BaseModel):
    symbol: str
    trade_date: date
    max_drawdown: Optional[float] = None
    var_95: Optional[float] = None
    cvar_95: Optional[float] = None
    calculated_at: datetime

@app.get("/wake")
def wake_and_ingest():
    """Explicit endpoint to wake up service and immediately ingest live Yahoo Finance ticks."""
    threading.Thread(target=auto_ingest_if_needed, kwargs={"force": True}, daemon=True).start()
    return {
        "status": "wake_triggered",
        "message": "Live Yahoo Finance ingestion cycle triggered on wake-up!",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.get("/health")
def health_check():
    # Trigger wake-up ingestion asynchronously if DB connection is healthy
    threading.Thread(target=auto_ingest_if_needed, kwargs={"force": False}, daemon=True).start()
    
    pg_host = os.getenv("POSTGRES_HOST", "localhost")
    pg_db = os.getenv("POSTGRES_DB", "neondb")
    pg_user = os.getenv("POSTGRES_USER", "neondb_owner")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        cur.close()
        conn.close()
        return {
            "status": "healthy",
            "database": "connected",
            "connected_host": pg_host,
            "connected_db": pg_db,
            "connected_user": pg_user,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "connected_host": pg_host,
            "connected_db": pg_db,
            "database_error": str(e)
        }

@app.get("/ticks/{symbol}", response_model=List[TickResponse])
def get_stock_ticks(symbol: str, limit: int = Query(default=100, le=1000)):
    # Trigger auto ingestion if ticks table hasn't been updated recently
    threading.Thread(target=auto_ingest_if_needed, kwargs={"force": False}, daemon=True).start()
    
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT symbol, timestamp, open, high, low, close, volume, quality_flag, processed_at
        FROM silver.cleaned_stock_ticks
        WHERE UPPER(symbol) = UPPER(%s)
        ORDER BY timestamp DESC
        LIMIT %s;
    """, (symbol, limit))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return clean_dict_rows(rows)

@app.get("/returns/{symbol}", response_model=List[DailyReturnResponse])
def get_daily_returns(symbol: str, limit: int = Query(default=100, le=1000)):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT symbol, trade_date, close, prev_close, daily_return, calculated_at
        FROM gold.daily_returns
        WHERE UPPER(symbol) = UPPER(%s)
        ORDER BY trade_date DESC
        LIMIT %s;
    """, (symbol, limit))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return clean_dict_rows(rows)

@app.get("/ma/{symbol}", response_model=List[MovingAverageResponse])
def get_moving_averages(symbol: str, limit: int = Query(default=100, le=1000)):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT symbol, trade_date, ma_20, ma_50, ma_200, calculated_at
        FROM gold.moving_averages
        WHERE UPPER(symbol) = UPPER(%s)
        ORDER BY trade_date DESC
        LIMIT %s;
    """, (symbol, limit))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return clean_dict_rows(rows)

@app.get("/volatility/{symbol}", response_model=List[VolatilityResponse])
def get_volatility_metrics(symbol: str, limit: int = Query(default=100, le=1000)):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT symbol, trade_date, rolling_volatility, annualized_volatility, calculated_at
        FROM gold.volatility_metrics
        WHERE UPPER(symbol) = UPPER(%s)
        ORDER BY trade_date DESC
        LIMIT %s;
    """, (symbol, limit))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return clean_dict_rows(rows)

@app.get("/sharpe/{symbol}", response_model=List[SharpeResponse])
def get_sharpe_metrics(symbol: str, limit: int = Query(default=100, le=1000)):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT symbol, trade_date, annualized_return, risk_free_rate, sharpe_ratio, calculated_at
        FROM gold.sharpe_metrics
        WHERE UPPER(symbol) = UPPER(%s)
        ORDER BY trade_date DESC
        LIMIT %s;
    """, (symbol, limit))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return clean_dict_rows(rows)

@app.get("/risk/{symbol}", response_model=List[RiskResponse])
def get_risk_metrics(symbol: str, limit: int = Query(default=100, le=1000)):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT symbol, trade_date, max_drawdown, var_95, cvar_95, calculated_at
        FROM gold.risk_metrics
        WHERE UPPER(symbol) = UPPER(%s)
        ORDER BY trade_date DESC
        LIMIT %s;
    """, (symbol, limit))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return clean_dict_rows(rows)
