#!/usr/bin/env python3
# ============================================================================
#  STEP 4 FILE — seeds + mutates the bank source DB so you can WATCH CDC work
#  ---------------------------------------------------------------------
#  WHAT THIS FILE WILL DO (we build it in Step 4):
#
#    1. Connect to postgres-source (localhost:5433, shop/shop, db shopdb)
#    2. UPSERT ~20 customers + ~10 merchants (so joins have rich matches)
#    3. Then, in a loop, every ~15 seconds make ONE realistic change:
#         - raise a customer's risk_segment
#         - flag/unflag a merchant as is_high_risk
#         - insert a brand-new customer
#       ...and print what it changed.
#    4. Because CDC is on, every one of those prints is followed seconds
#       later by a Kafka event and an Iceberg MERGE — visible end-to-end.
#
#  RUN (once built):   python3 scripts/seed_customers.py    (or: make seed)
# ============================================================================
