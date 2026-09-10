-- ============================================================================
--  Runs ONCE, the first time the "postgres-source" (bank) container starts.
--  This database pretends to be the bank's real OLTP system: the place where
--  customer and merchant master data is created and edited by the business.
--
--  The last statement (CREATE PUBLICATION) is what lets Debezium stream
--  every INSERT/UPDATE/DELETE on these tables into Kafka (CDC).
-- ============================================================================

CREATE TABLE customers (
    customer_id   TEXT PRIMARY KEY,
    full_name     TEXT NOT NULL,
    email         TEXT,
    age           INT,
    home_country  TEXT,
    risk_segment  TEXT,                          -- low | medium | high
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE merchants (
    merchant_id       TEXT PRIMARY KEY,
    merchant_name     TEXT NOT NULL,
    category          TEXT,                      -- grocery, fuel, electronics...
    country           TEXT,
    is_high_risk      BOOLEAN DEFAULT FALSE,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Seed data so the streaming join has something to match on day one.
INSERT INTO customers (customer_id, full_name, email, age, home_country, risk_segment) VALUES
  ('CUST-001','Asha Rao','asha@example.com',34,'IN','low'),
  ('CUST-002','Ben Carter','ben@example.com',41,'US','low'),
  ('CUST-003','Chen Wei','chen@example.com',29,'SG','medium'),
  ('CUST-004','Dana Lee','dana@example.com',52,'US','high'),
  ('CUST-005','Omar Farouk','omar@example.com',38,'AE','medium');

INSERT INTO merchants (merchant_id, merchant_name, category, country, is_high_risk) VALUES
  ('MER-100','FreshMart','grocery','US',FALSE),
  ('MER-200','SpeedFuel','fuel','US',FALSE),
  ('MER-300','GadgetHub','electronics','SG',TRUE),
  ('MER-400','LuxTime','jewellery','AE',TRUE),
  ('MER-500','BookNook','retail','IN',FALSE);

-- THE CDC SWITCH: publish these two tables on the logical replication slot.
-- Debezium will subscribe to this publication and emit change events to Kafka.
CREATE PUBLICATION debezium_pub FOR TABLE customers, merchants;
