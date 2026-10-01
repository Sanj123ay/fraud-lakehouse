#!/usr/bin/env python3
"""Seed & Mutate Bank OLTP Database (Triggers Debezium CDC).
Usage:
  python3 scripts/seed_customers.py           # Runs in live mutation loop (every 10s)
  python3 scripts/seed_customers.py --seed    # Seeds master records once and exits
  python3 scripts/seed_customers.py --mutate  # Executes a single random mutation and exits
Works directly on host (falls back to `docker compose exec` if psycopg2 is not installed).
"""
from __future__ import annotations
import argparse
import os
import random
import subprocess
import sys
import time
from datetime import datetime
# Connection settings
PG_HOST = os.getenv("SOURCE_DB_HOST", "localhost")
PG_PORT = os.getenv("SOURCE_DB_PORT", "5433")
PG_USER = os.getenv("SOURCE_DB_USER", "shop")
PG_PASS = os.getenv("SOURCE_DB_PASSWORD", "shop")
PG_DB = os.getenv("SOURCE_DB_NAME", "shopdb")
INITIAL_CUSTOMERS = [
    ("CUST-001", "Asha Rao", "asha@example.com", 34, "IN", "low"),
    ("CUST-002", "Ben Carter", "ben@example.com", 41, "US", "low"),
    ("CUST-003", "Chen Wei", "chen@example.com", 29, "SG", "medium"),
    ("CUST-004", "Dana Lee", "dana@example.com", 52, "US", "high"),
    ("CUST-005", "Omar Farouk", "omar@example.com", 38, "AE", "medium"),
    ("CUST-006", "Elena Rostova", "elena@example.com", 27, "DE", "low"),
    ("CUST-007", "Marcus Aurelius", "marcus@example.com", 45, "GB", "low"),
    ("CUST-008", "Fatima Al-Sayed", "fatima@example.com", 31, "AE", "low"),
    ("CUST-009", "Hiroshi Tanaka", "hiroshi@example.com", 36, "SG", "low"),
    ("CUST-010", "Priya Sharma", "priya@example.com", 24, "IN", "medium"),
]
INITIAL_MERCHANTS = [
    ("MER-100", "FreshMart", "grocery", "US", False),
    ("MER-200", "SpeedFuel", "fuel", "US", False),
    ("MER-300", "GadgetHub", "electronics", "SG", True),
    ("MER-400", "LuxTime", "jewellery", "AE", True),
    ("MER-500", "BookNook", "retail", "IN", False),
    ("MER-600", "CloudAir", "travel", "DE", True),
    ("MER-700", "MetroDine", "restaurant", "GB", False),
    ("MER-800", "ElectroWorld", "electronics", "IN", True),
]
def execute_sql(sql: str) -> None:
    """Execute SQL query using psycopg2 if available, or via docker compose exec psql."""
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASS,
            dbname=PG_DB,
            connect_timeout=5,
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.close()
    except ImportError:
        # Fallback to docker compose CLI
        cmd = [
            "docker", "compose", "exec", "-T", "postgres-source",
            "psql", "-U", PG_USER, "-d", PG_DB, "-c", sql,
        ]
        res = subprocess.run(cmd, capture_output=True, text=True)
        if res.returncode != 0:
            raise RuntimeError(f"psql execution failed: {res.stderr}")
    except Exception as err:
        # If host port 5433 fails, try container execution
        cmd = [
            "docker", "compose", "exec", "-T", "postgres-source",
            "psql", "-U", PG_USER, "-d", PG_DB, "-c", sql,
        ]
        res = subprocess.run(cmd, capture_output=True, text=True)
        if res.returncode != 0:
            raise RuntimeError(f"Database execution error: {err}\nFallback failed: {res.stderr}")
def seed_master_data() -> None:
    """Insert or upsert all initial customer and merchant master records."""
    print("🌱 Seeding bank master records in postgres-source (shopdb)...")
    for cid, name, email, age, country, risk in INITIAL_CUSTOMERS:
        sql = f"""
        INSERT INTO customers (customer_id, full_name, email, age, home_country, risk_segment, updated_at)
        VALUES ('{cid}', '{name}', '{email}', {age}, '{country}', '{risk}', now())
        ON CONFLICT (customer_id) DO UPDATE SET
            full_name = EXCLUDED.full_name,
            email = EXCLUDED.email,
            age = EXCLUDED.age,
            home_country = EXCLUDED.home_country,
            risk_segment = EXCLUDED.risk_segment,
            updated_at = now();
        """
        execute_sql(sql)
    for mid, name, cat, country, high_risk in INITIAL_MERCHANTS:
        sql = f"""
        INSERT INTO merchants (merchant_id, merchant_name, category, country, is_high_risk, updated_at)
        VALUES ('{mid}', '{name}', '{cat}', '{country}', {'TRUE' if high_risk else 'FALSE'}, now())
        ON CONFLICT (merchant_id) DO UPDATE SET
            merchant_name = EXCLUDED.merchant_name,
            category = EXCLUDED.category,
            country = EXCLUDED.country,
            is_high_risk = EXCLUDED.is_high_risk,
            updated_at = now();
        """
        execute_sql(sql)
    print(f"✅ Seeded {len(INITIAL_CUSTOMERS)} customers and {len(INITIAL_MERCHANTS)} merchants.")
def perform_random_mutation() -> str:
    """Perform one random realistic modification to simulate OLTP operational activity."""
    action = random.choice(["update_customer_risk", "toggle_merchant_risk", "new_customer"])
    ts = datetime.utcnow().strftime("%H:%M:%S")
    if action == "update_customer_risk":
        cid, name, _, _, _, _ = random.choice(INITIAL_CUSTOMERS)
        new_risk = random.choice(["low", "medium", "high"])
        sql = f"UPDATE customers SET risk_segment = '{new_risk}', updated_at = now() WHERE customer_id = '{cid}';"
        execute_sql(sql)
        desc = f"[{ts}] 🔄 UPDATED customer {cid} ({name}) -> risk_segment='{new_risk}'"
    elif action == "toggle_merchant_risk":
        mid, name, _, _, _ = random.choice(INITIAL_MERCHANTS)
        new_flag = random.choice(["TRUE", "FALSE"])
        sql = f"UPDATE merchants SET is_high_risk = {new_flag}, updated_at = now() WHERE merchant_id = '{mid}';"
        execute_sql(sql)
        desc = f"[{ts}] ⚡ UPDATED merchant {mid} ({name}) -> is_high_risk={new_flag}"
    else:
        new_id = f"CUST-{random.randint(100, 999)}"
        sql = f"""
        INSERT INTO customers (customer_id, full_name, email, age, home_country, risk_segment, updated_at)
        VALUES ('{new_id}', 'New User {new_id}', '{new_id.lower()}@example.com', {random.randint(20, 60)}, 'US', 'low', now())
        ON CONFLICT (customer_id) DO UPDATE SET risk_segment = 'low', updated_at = now();
        """
        execute_sql(sql)
        desc = f"[{ts}] ➕ INSERTED new customer {new_id}"
    return desc
def main() -> None:
    parser = argparse.ArgumentParser(description="Seed & mutate bank database to trigger CDC.")
    parser.add_argument("--seed", action="store_true", help="Seed master data once and exit.")
    parser.add_argument("--mutate", action="store_true", help="Perform a single mutation and exit.")
    parser.add_argument("--interval", type=int, default=10, help="Interval (seconds) between mutations in loop mode (default: 10).")
    args = parser.parse_args()
    seed_master_data()
    if args.seed:
        return
    if args.mutate:
        desc = perform_random_mutation()
        print(desc)
        return
    print(f"\n🚀 Running live CDC mutation loop (1 change every {args.interval}s). Press Ctrl+C to stop.\n")
    try:
        while True:
            desc = perform_random_mutation()
            print(f"{desc}  --> [Captured by Debezium WAL -> Kafka -> Spark Iceberg MERGE]")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nStopped CDC mutation loop.")
if __name__ == "__main__":
    main()
