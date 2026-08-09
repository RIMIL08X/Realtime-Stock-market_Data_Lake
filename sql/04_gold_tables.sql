-- 04_gold_tables.sql

-- 1. Daily Returns
CREATE TABLE IF NOT EXISTS gold.daily_returns (
    symbol VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    close NUMERIC(18,6) NOT NULL,
    prev_close NUMERIC(18,6),
    daily_return NUMERIC(10,8),
    calculated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, trade_date)
);

-- 2. Moving Averages
CREATE TABLE IF NOT EXISTS gold.moving_averages (
    symbol VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    ma_20 NUMERIC(18,6),
    ma_50 NUMERIC(18,6),
    ma_200 NUMERIC(18,6),
    calculated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, trade_date)
);

-- 3. Volatility Metrics
CREATE TABLE IF NOT EXISTS gold.volatility_metrics (
    symbol VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    rolling_volatility NUMERIC(10,8),
    annualized_volatility NUMERIC(10,8),
    calculated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, trade_date)
);

-- 4. Sharpe Metrics
CREATE TABLE IF NOT EXISTS gold.sharpe_metrics (
    symbol VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    annualized_return NUMERIC(10,8),
    risk_free_rate NUMERIC(6,4) DEFAULT 0.0450,
    sharpe_ratio NUMERIC(10,6),
    calculated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, trade_date)
);

-- 5. Risk Metrics
CREATE TABLE IF NOT EXISTS gold.risk_metrics (
    symbol VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    max_drawdown NUMERIC(10,8),
    var_95 NUMERIC(10,8),
    cvar_95 NUMERIC(10,8),
    calculated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, trade_date)
);
