# ============================================================================
#  STEP 5 FILE — nightly batch training (run by Airflow, or `make train`)
#  ---------------------------------------------------------------------
#  WHAT THIS FILE WILL DO (we build it in Step 5):
#
#    1. READ    historical labelled data:
#                 spark.table("lakehouse.raw.transactions")
#               (the generator's planted fraud patterns = the label is_fraud)
#    2. FEaturize with the SAME feature definitions as streaming (critical:
#               training/serving skew is a classic production bug — we use the
#               shared rolling features stored in lakehouse.features.*)
#    3. PIPELINE Spark ML Pipeline:
#                 StringIndexer  (channel, country, risk_segment, category)
#                 VectorAssembler(amount, count_5min, amount_sum_1h, ... )
#                 RandomForestClassifier(numTrees=100, maxDepth=8)
#    4. SPLIT   time-based split (train on older days, validate on recent)
#    5. EVALUATE BinaryClassificationEvaluator -> AUC; print a confusion matrix
#    6. CHAMPION/CHALLENGER  compare new AUC vs the current model's stored
#               metric; only publish if better:
#                 model.write().overwrite().save("s3a://models/fraud_rf/latest")
#               (+ a small JSON sidecar with metrics = poor-man's MLflow;
#                docs note how to swap in real MLflow later)
#    7. LOG     append run info into fraud_ops predictions metadata table
#
#  RUN (once built):   make train        (or let the nightly DAG do it)
# ============================================================================
