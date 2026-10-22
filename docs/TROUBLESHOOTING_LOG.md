# Troubleshooting log

A running record of every real error we hit while building and running this platform:
the message, the cause, and the fix. Add a new entry each time something breaks, so the
next person (or you, next month) can find it quickly.

**Entry template**

```text
## E-XXX — short title
- Date / step:
- Where it showed up: (make target, Airflow task, UI, ...)
- Error message: (paste the key line)
- Cause: (why it happened, in plain words)
- Fix: (what we changed, with the file name)
- Status: fixed / open
```

---

## E-001 — Airflow: `TemplateNotFound: bash /opt/airflow/spark_jobs/batch/submit_training.sh`

- **Date / step:** 2026-10-22, Step 5 (`dag_01_daily_training`)
- **Where it showed up:** Airflow task `train_fraud_model`, during task start-up
  (before any Spark code ran).
- **Error message:**
  ```text
  jinja2.exceptions.TemplateNotFound: bash /opt/airflow/spark_jobs/batch/submit_training.sh
  ```
- **Cause:** `BashOperator` has a feature called *templating*: it fills `{{ ... }}`
  placeholders in the command before running it. When the command ends in `.sh` or `.bash`,
  it assumes the whole string is the *path of a template file* and tries to load it. Our
  command was `bash <path>.sh`, so Airflow looked for a file literally named
  `bash /opt/airflow/...sh`, found nothing, and failed. The script itself never ran.
- **Fix:** `airflow/dags/dag_01_daily_training.py` now uses a multi-line command that ends
  with a newline, so it no longer ends in `.sh`:
  ```python
  bash_command="set -e\nbash /opt/airflow/spark_jobs/batch/submit_training.sh\n",
  ```
  `set -e` makes the shell stop at the first failing line.
- **Status:** fixed (pending re-run in Airflow)

**Plain-English lesson:** Airflow reads your command text before it runs it. Anything that
looks like a file name ending in `.sh` gets treated as a file to load. When a command
looks wrong to Airflow, check the end of the string first.

---

## E-002 — `make` not recognised on Windows PowerShell

- **Date / step:** 2026-10-22, setup (before Step 1 was complete)
- **Where it showed up:** PowerShell, running `make up`
- **Error message:** `make : The term 'make' is not recognized as the name of a cmdlet ...`
- **Cause:** Windows does not ship with `make`. The Makefile is only shortcuts for Docker
  commands.
- **Fix:** Run the equivalent command directly (`docker compose up -d --build`), or install
  `make` with `winget install GnuWin32.Make`. Shell-script targets (`make topics`,
  `make register-cdc`) need Git Bash.
- **Status:** worked around (documented in chat; not a code change)

---

## E-003 — Training job reports AUC 1.0 (a result, not a crash)

- **Date / step:** 2026-10-22, Step 5 (`make train`)
- **Where it showed up:** Training output: `"auc_roc": 1.0`, `Decision: published`
- **Cause:** The data generator plants fraud with amounts of 700–9,000, while normal
  amounts are mostly under 100. The fraud is therefore trivially separable, so the model
  scores perfectly. No label leakage: `is_fraud` is only used as the label.
- **Fix:** Open. Decision needed: make the simulated fraud harder to separate (Step 2 generator).
- **Status:** open

---

## E-004 — Two model features have zero importance

- **Date / step:** 2026-10-22, Step 5 (`inspect-model`)
- **Where it showed up:** Feature importance list: `country_mismatch_f` and
  `merchant_high_risk_f` are both `0.0000`.
- **Cause (suspected):** Those columns may be always null or constant, which would mean the
  CDC-fed dimension join is not populating them. Not yet confirmed.
- **Fix:** Open. Check with `make inspect-step4`.
- **Status:** open
