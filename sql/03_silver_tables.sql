-- 03_silver_tables.sql
CREATE TABLE IF NOT EXISTS silver.cleaned_stock_ticks (
    id BIGSERIAL PRIMARY KEY,
    bronze_id BIGINT REFERENCES bronze.stock_ticks(id) ON DELETE SET NULL,
    symbol VARCHAR(20) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    open NUMERIC(18,6) NOT NULL,
    high NUMERIC(18,6) NOT NULL,
    low NUMERIC(18,6) NOT NULL,
    close NUMERIC(18,6) NOT NULL,
    volume BIGINT NOT NULL,
    quality_flag VARCHAR(10) NOT NULL CHECK (quality_flag IN ('pass', 'warn', 'fail')),
    processed_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uk_silver_symbol_timestamp UNIQUE (symbol, timestamp),
    CONSTRAINT chk_silver_high_low CHECK (high >= low),
    CONSTRAINT chk_silver_close_positive CHECK (close > 0)
);

CREATE INDEX IF NOT EXISTS idx_silver_symbol_timestamp ON silver.cleaned_stock_ticks (symbol, timestamp DESC);
