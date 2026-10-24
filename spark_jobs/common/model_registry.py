"""
common/model_registry.py — load the champion fraud model for live scoring (Step 6).

The streaming job calls `load_champion()` on every micro-batch. Loading a model
from MinIO on every 10-second batch would be wasteful, so this module:

  * keeps the loaded PipelineModel in memory (module-level cache), and
  * checks MinIO for a newer champion at most every MODEL_RELOAD_SECONDS.

Training (Step 5) writes the model first and the metrics JSON second, so when a
new version number appears in the metrics, the model files are already complete.
"""
from __future__ import annotations

import time

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession

from common.config import Settings
from common.storage import read_json

_cache: dict = {"model": None, "version": None, "checked_at": None}


def load_champion(spark: SparkSession, settings: Settings):
    """Return (PipelineModel, model_version), or None if no champion exists yet."""
    now = time.monotonic()
    checked_at = _cache["checked_at"]
    if (
        _cache["model"] is not None
        and checked_at is not None
        and now - checked_at < settings.MODEL_RELOAD_SECONDS
    ):
        return _cache["model"], _cache["version"]

    _cache["checked_at"] = now
    metrics = read_json(spark, settings.MODEL_METRICS_PATH)
    if metrics is None:
        return None

    version = metrics.get("model_version")
    if version != _cache["version"]:
        try:
            model = PipelineModel.load(settings.MODEL_PATH)
        except Exception as exc:  # keep serving the previous champion if the new one is unreadable
            print(f"[model] WARNING: could not load champion {version}: {exc}", flush=True)
            if _cache["model"] is None:
                return None
            return _cache["model"], _cache["version"]
        _cache.update(model=model, version=version)
        print(f"[model] loaded champion model version {version}", flush=True)

    return _cache["model"], _cache["version"]
