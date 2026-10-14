"""
common/model_features.py — the ONE definition of model input features.

Why this file exists
--------------------
The classic production bug in ML is "training/serving skew": the model is
trained on one version of a feature and scored on a slightly different one.
To prevent that, the feature logic lives here as a Spark ML *pipeline stage*.

  * Step 5 (job2_train_model.py) puts these stages in front of the classifier
    and saves the whole PipelineModel to MinIO.
  * Step 6 (job1_streaming_features.py) loads that PipelineModel and calls
    .transform() on live micro-batches. The exact same SQL runs in both places.

Every input column is non-null and numeric after this stage, so the model never
sees a surprise NULL. Unknown categories at serving time map to an extra index
instead of crashing (handleInvalid="keep").
"""
from __future__ import annotations

from pyspark.ml.feature import SQLTransformer, StringIndexer, VectorAssembler

# SQL run on every batch. __THIS__ is replaced by Spark with the input DataFrame.
FEATURE_SQL = """
SELECT
    __THIS__.*,
    CAST(COALESCE(amount, 0.0) AS DOUBLE)              AS amount_f,
    CAST(COALESCE(count_5min, 0) AS DOUBLE)            AS count_5min_f,
    CAST(COALESCE(amount_sum_1h, 0.0) AS DOUBLE)       AS amount_sum_1h_f,
    CASE WHEN country_mismatch THEN 1.0 ELSE 0.0 END   AS country_mismatch_f,
    CASE WHEN merchant_is_high_risk THEN 1.0 ELSE 0.0 END AS merchant_high_risk_f,
    CAST(hour(event_time) AS DOUBLE)                   AS hour_of_day_f,
    COALESCE(channel, 'unknown')                       AS channel_c,
    COALESCE(customer_risk_segment, 'unknown')         AS risk_segment_c
FROM __THIS__
"""

NUMERIC_INPUTS = [
    "amount_f",
    "count_5min_f",
    "amount_sum_1h_f",
    "country_mismatch_f",
    "merchant_high_risk_f",
    "hour_of_day_f",
]

# input column (cleaned text) -> output column (numeric index)
CATEGORICAL_INPUTS = {
    "channel_c": "channel_idx",
    "risk_segment_c": "risk_segment_idx",
}

# Order matters: it is the order of feature importances printed after training.
MODEL_FEATURE_COLUMNS = NUMERIC_INPUTS + list(CATEGORICAL_INPUTS.values())


def build_feature_stages() -> list:
    """Return fresh Spark ML stages that turn raw/feature columns into `features`."""
    sql_stage = SQLTransformer(statement=FEATURE_SQL)
    index_stages = [
        StringIndexer(inputCol=source, outputCol=target, handleInvalid="keep")
        for source, target in CATEGORICAL_INPUTS.items()
    ]
    assembler = VectorAssembler(inputCols=MODEL_FEATURE_COLUMNS, outputCol="features")
    return [sql_stage, *index_stages, assembler]
