import os
import logging
from typing import List, Optional
from datetime import date, datetime
from decimal import Decimal

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger("FinancialDataAPI")

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
    pg_host = os.getenv("POSTGRES_HOST", "localhost")
    pg_port = int(os.getenv("POSTGRES_PORT", "5433"))
    pg_db = os.getenv("POSTGRES_DB", "market_db")
    pg_user = os.getenv("POSTGRES_USER", "market_user")
    pg_pass = os.getenv("POSTGRES_PASSWORD", "market_pass")

    try:
        conn = psycopg2.connect(
            host=pg_host,
            port=pg_port,
            dbname=pg_db,
            user=pg_user,
            password=pg_pass,
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

@app.get("/health")
def health_check():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        cur.close()
        conn.close()
        return {"status": "healthy", "database": "connected", "timestamp": datetime.utcnow().isoformat()}
    except Exception as e:
        return {"status": "unhealthy", "database_error": str(e)}

@app.get("/ticks/{symbol}", response_model=List[TickResponse])
def get_stock_ticks(symbol: str, limit: int = Query(default=100, le=1000)):
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
