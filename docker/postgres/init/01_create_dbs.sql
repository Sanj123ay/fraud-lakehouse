-- ============================================================================
--  Runs ONCE, the first time the "postgres" (ops) container starts.
--  Postgres automatically executes every *.sql in /docker-entrypoint-initdb.d.
--
--  airflow_meta database is created automatically by the container env var.
--  Here we add our own "fraud_ops" database and the predictions table.
-- ============================================================================

CREATE DATABASE fraud_ops;

\c fraud_ops

-- Real-time fraud predictions land here (written by Spark streaming job1).
-- Analysts / dashboards query this table.
CREATE TABLE IF NOT EXISTS predictions (
    txn_id          TEXT PRIMARY KEY,          -- unique transaction id
    event_time      TIMESTAMPTZ NOT NULL,      -- when the swipe happened
    card_id         TEXT        NOT NULL,
    customer_id     TEXT,
    merchant_id     TEXT,
    amount          NUMERIC(12,2) NOT NULL,
    fraud_score     DOUBLE PRECISION NOT NULL, -- model output 0.0 .. 1.0
    is_fraud_pred   BOOLEAN NOT NULL,          -- thresholded decision
    model_version   TEXT,                      -- which model produced this
    scored_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_predictions_card ON predictions (card_id, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_predictions_flag ON predictions (is_fraud_pred) WHERE is_fraud_pred;
