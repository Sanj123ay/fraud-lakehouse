#!/usr/bin/env python3
"""Validate newline-delimited transaction JSON read from Kafka.
Usage (normally through `make verify-ingestion`):
    kafka-console-consumer ... | python3 scripts/validate_transactions.py
Uses only Python's standard library, so nothing needs to be installed locally.
"""
from __future__ import annotations
import json
import sys
from datetime import datetime
from uuid import UUID
REQUIRED_FIELDS = {
    "txn_id",
    "card_id",
    "customer_id",
    "merchant_id",
    "amount",
    "currency",
    "country",
    "channel",
    "txn_ts",
    "is_fraud",
}
ALLOWED_CHANNELS = {"pos", "ecom", "atm"}
ALLOWED_CUSTOMERS = {f"CUST-{number:03d}" for number in range(1, 6)}
ALLOWED_MERCHANTS = {f"MER-{number}" for number in (100, 200, 300, 400, 500)}

def validate(record: object, line_number: int) -> dict:
    if not isinstance(record, dict):
        raise ValueError(f"line {line_number}: expected a JSON object")
    missing = REQUIRED_FIELDS - record.keys()
    extra = record.keys() - REQUIRED_FIELDS
    if missing:
        raise ValueError(f"line {line_number}: missing fields {sorted(missing)}")
    if extra:
        raise ValueError(f"line {line_number}: unexpected fields {sorted(extra)}")
    UUID(record["txn_id"])
    if not isinstance(record["card_id"], str) or not record["card_id"].startswith("CARD-"):
        raise ValueError(f"line {line_number}: invalid card_id")
    if record["customer_id"] not in ALLOWED_CUSTOMERS:
        raise ValueError(f"line {line_number}: invalid customer_id")
    if record["merchant_id"] not in ALLOWED_MERCHANTS:
        raise ValueError(f"line {line_number}: invalid merchant_id")
    if isinstance(record["amount"], bool) or not isinstance(record["amount"], (int, float)):
        raise ValueError(f"line {line_number}: amount must be numeric")
    if not 0 < record["amount"] <= 9000:
        raise ValueError(f"line {line_number}: amount outside expected range")
    if record["channel"] not in ALLOWED_CHANNELS:
        raise ValueError(f"line {line_number}: invalid channel")
    if record["is_fraud"] not in (0, 1):
        raise ValueError(f"line {line_number}: is_fraud must be 0 or 1")
    # fromisoformat understands +00:00; convert the common trailing Z first.
    parsed_time = datetime.fromisoformat(record["txn_ts"].replace("Z", "+00:00"))
    if parsed_time.tzinfo is None:
        raise ValueError(f"line {line_number}: txn_ts must include a timezone")
    for field in ("currency", "country"):
        if not isinstance(record[field], str) or not record[field]:
            raise ValueError(f"line {line_number}: {field} must be a non-empty string")
    return record
def main() -> int:
    records: list[dict] = []
    for line_number, raw_line in enumerate(sys.stdin, start=1):
        line = raw_line.strip()
        if not line:
            continue
        try:
            records.append(validate(json.loads(line), line_number))
        except (ValueError, TypeError, json.JSONDecodeError) as error:
            print(f"FAILED: {error}", file=sys.stderr)
            print(f"Offending input: {line[:500]}", file=sys.stderr)
            return 1
    if not records:
        print(
            "FAILED: Kafka returned no records. Start the NiFi processors and retry.",
            file=sys.stderr,
        )
        return 1
    fraud_count = sum(record["is_fraud"] for record in records)
    print(f"PASS: validated {len(records)} schema-compatible Kafka transactions.")
    print(f"Sample transaction id: {records[0]['txn_id']}")
    print(f"Fraud-labelled rows in this sample: {fraud_count}/{len(records)}")
    return 0
if __name__ == "__main__":
    raise SystemExit(main())
