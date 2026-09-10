// ============================================================================
//  STEP 2 FILE — NiFi ExecuteScript (Groovy): fake transaction generator
//  ---------------------------------------------------------------------
//  WHAT THIS FILE WILL CONTAIN (we write it together in Step 2):
//
//  A Groovy script that fabricates one realistic credit-card transaction
//  per run and returns it as a JSON string, e.g.:
//
//  {
//    "txn_id": "a1b2c3...",          // random UUID
//    "card_id": "CARD-42",            // small pool -> some cards get hot
//    "customer_id": "CUST-002",       // matches seed data in the bank DB
//    "merchant_id": "MER-300",        // matches seed merchants
//    "amount": 129.50,                // log-normal-ish; a few HUGE ones
//    "currency": "USD",
//    "country": "US",                 // occasionally != customer's home
//    "channel": "pos|ecom|atm",
//    "txn_ts": "2026-01-15T03:44:12Z",
//    "is_fraud": 0                    // HIDDEN LABEL used later for training;
//  }                                  // injected patterns: night + abroad +
//                                     // high amount + rapid repeats = 1
//
//  WHY WE DELIBERATELY PLANT FRAUD PATTERNS:
//  So the ML model in Step 5 has real signal to learn, and the streaming
//  scores in Step 6 light up on screen when an "attack" happens.
//
//  The NiFi flow around this script is documented in ../flow/README.md.
// ============================================================================
