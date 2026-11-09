# Saffron Shield — Claims Data Platform

An end-to-end health-insurance claims lakehouse, packaged as a **Declarative Automation Bundle (DAB)**. This README is written so that someone who has never seen this project can set up the tooling, bootstrap the data environment, deploy the bundle, and run the validation tests — using only this document.

## 1. What this is

A medallion-style claims pipeline on Databricks:

```
Landing volume (CSV / JSON)
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│ Daily job: Saffron_shield_daily_pipeline                         │
│ (triggered automatically when a new claims file lands)           │
│                                                                  │
│  1. bronze    – 01_bronze notebook: Auto Loader ingest           │
│  2. Silver    – claims_pipeline (Spark Declarative Pipeline):    │
│                 validate + quarantine, publish claims_clean      │
│  3. policy    – policy notebook: CDC → SCD2 policy dimension     │
│  4. Gold      – Gold notebook: point-in-time enriched facts      │
│  5. Designer  – monthly hospital summary (Visual data prep)      │
└─────────────────────────────────────────────────────────────────┘
        │
        ▼
Governance layer (governance notebook): PII masks, state-scoped
row filters, PII tags, table/column comments, change audit
```

## 2. Prerequisites

1. **A Databricks workspace** with Unity Catalog (shared or dedicated serverless is enough; all notebooks here run on serverless — no cluster config needed).
2. **Unity Catalog permissions** — you must be able to `CREATE CATALOG` (or be granted `USE CATALOG` on an existing `claims` catalog), `CREATE SCHEMA`, and write to a UC volume.
3. **Databricks CLI** installed and authenticated:

   ```bash
   # install (any one)
   pip install databricks     # or: brew install databricks

   # authenticate — opens a browser; pick your workspace
   # https://dbc-e63a3b85-204d.cloud.databricks.com
   databricks auth login

   # sanity check
   databricks current-user me
   ```

4. **(For AI coding agents working on this repo)** the project's `AGENTS.md` asks for the Databricks AI tools:

   ```bash
   databricks aitools install
   ```

5. **(For the pytest harness, section 8.4)** [uv](https://docs.astral.sh/uv/) — installs `pytest` + `databricks-connect` from `pyproject.toml`.

## 3. One-time environment bootstrap

### 3.1 Land the raw files

Put the source files in the `raw.landing` volume (created by the setup notebook below):

| File / folder under `/Volumes/claims/raw/landing/` | Format | Used by |
| --- | --- | --- |
| `plans.csv` | CSV, header | reference tables (00_setup) |
| `hospitals.csv` | CSV, header | reference tables (00_setup) |
| `members.csv` | CSV, header | reference tables (00_setup) |
| `hospital_network_history.csv` | CSV, header | reference tables (00_setup) |
| `claims_raw/` | JSON, one claim per file | Auto Loader (01_bronze) — **the file-arrival trigger watches this folder** |
| `policy_changes_cdc/` | JSON | CDC source for the policy dimension |

`claims/checkpoints/` and `claims/schema` are created automatically by Auto Loader — do not put anything there.

### 3.2 Run the setup notebook

Open `src/00_setup` and run all cells (serverless SQL). It is idempotent (`create ... if not exists`):

* Creates catalog `claims` and schemas `raw`, `bronze`, `silver`, `gold`.
* Creates volume `raw.landing`.
* Creates reference tables `raw.plans`, `raw.hospitals`, `raw.members`, `raw.hospital_network_history` from the landing CSVs.

If you are starting from a truly empty workspace, run this **before** the first deploy so the job has something to read.

## 4. The Silver layer: `claims_pipeline` (Spark Declarative Pipeline)

The silver layer is a standalone SDP pipeline, **`claims_pipeline`** (id `f733c441-fb81-46a5-a318-c406d090aeed`, catalog `claims`, schema `silver`, libraries glob `src/claims_pipeline/transformations/**`). It reads `bronze.claims` and `raw.hospitals` / `raw.members` and publishes two materialized views:

* **`silver.claims_clean`** — one row per claim (latest `submitted_ts` per `claim_id`), every validation rule passed. Each rule is attached as an expectation, so per-rule pass/fail metrics are visible in the pipeline's data-quality tab.
* **`silver.claims_quarantine`** — claims that failed one or more rules, tagged with every broken rule in `reject_reasons`. **Nothing is ever silently dropped.**

The rule dictionary lives in `src/claims_pipeline/transformations/my_transformation.py` as `VALIDATION_RULES` (dates within 2025-07-01…2026-06-30, `admission_date <= discharge_date`, `settled_ts >= submitted_ts`, `approved_amount <= billed_amount`, `room_days = datediff(discharge, admission)` clamped to ≥ 1, status ∈ {submitted, approved, rejected, settled}, per-status field consistency, and member/hospital referential checks against `raw.members` / `raw.hospitals`). Edit rules there — the same dict drives both the quarantine tags and the expectations.

Run it manually with `databricks pipelines start f733c441-fb81-46a5-a318-c406d090aeed` (or Runs → Start), or let the daily job do it (task `Silver`).

> **Deploy note:** the job references this pipeline by its hard-coded `pipeline_id`. If you rebuild the pipeline in another workspace, update `resources/saffron_shield_daily_pipleine.job.yml` (task `Silver`) with the new id — or better, define the pipeline as a bundle resource and use `{{resources.pipelines.<name>.id}}`.

## 5. Policy dimension (SCD2)

`src/policy` loads `policy_changes_cdc/` into `bronze.policy` with `COPY INTO`, then:

1. Deduplicates CDC records per (member, policy, effective_from) keeping the latest `op_ts`.
2. Builds `effective_till` (lead of next `effective_from`) and `is_current` flags; drops `op = 'DELETE'` rows.
3. `MERGE`s into `silver.dim_policy_scd2` on (member_id, policy_id, effective_from) — so re-running the notebook is idempotent.

Result: one row per policy version. `effective_till IS NULL` means still current. The dimension carries only `plan_code`; plan attributes (sum insured, room-rent cap, copay %, waiting periods, premium) must be joined from `raw.plans`.

## 6. Gold layer

`src/Gold` builds (parameterized by the `catalog` widget, default `claims`):

* **`gold.claims_enriched`** — claim facts enriched with the plan **in force on the claim's own admission date** (SCD2 as-of join: `effective_from <= admission_date` and `effective_till IS NULL OR admission_date <= effective_till`) and whether the hospital was **in network at admission** (window join against `raw.hospital_network_history`, handling hospitals that left and rejoined). Facts only — no eligibility or payability verdicts, by design.
* **`gold.hospital_monthly`** — hospital × month aggregates derived from `claims_enriched`, so the two agree by construction.
* **`gold.hospital_monthly_designer`** — produced by `src/monthly_hospital_summary.designer.ipynb` (Visual data prep, task `Designer`).

## 7. Deploy the bundle

All commands run from the **bundle root** `Saffron_Shield/` (the folder containing `databricks.yml`).

```bash
cd Saffron_Shield

# 1. validate
bundle validate --target dev          # or: databricks bundle validate --target dev

# 2. deploy dev (development mode: names get a '[dev <user>]' prefix,
#    resources land under ~/.bundle/Saffron_Shield/dev)
bundle deploy --target dev

# 3. deploy prod (production mode: real names, single copy,
#    root_path is pinned in the prod target)
bundle deploy --target prod
```

Target variables: `dev` → `catalog: claims`, `prod` → `catalog: prod`. The job's `catalog` parameter is populated from `${var.catalog}`.

**Destructive-change prompts:** if a deploy would delete or recreate a pipeline (e.g. its `catalog`/`storage` changed, or the resource was removed from the bundle), the CLI asks for confirmation. In non-interactive environments add `--auto-approve` **only after reading the printed plan** — deletion of a pipeline forces a full refresh of the streaming tables and materialized views it manages.

## 8. Run the pipeline & the tests

### 8.1 Trigger a run

The deployed job `Saffron_shield_daily_pipeline` fires automatically when a new file lands in `/Volumes/claims/raw/landing/claims_raw/` (file-arrival trigger). To run on demand:

```bash
bundle run Saffron_shield_daily_pipeline --target dev
```

Tasks run in order: `bronze` → `Silver` → `policy` → `Gold` → `Designer`. `bronze` uses `trigger(availableNow=True)`, so each run processes exactly the files that arrived since the last checkpoint and then stops.

### 8.2 Tests (run after any rebuild)

Run these cells in order — every check is a cell that already exists in the notebooks:

| # | Test | Where | Passes when |
| --- | --- | --- | --- |
| 1 | Fact-vs-summary reconciliation | `Gold` → "Reconcile fact vs summary" | `fact_rows = summary_claim_count_total`, `fact_rejected_total = summary_rejected_count_total`, billed and approved sums match pairwise (at the 2026-11-05 build: 2,976 rows, 397 rejected, billed 288,418,853.52 — values move with new data; the **equality** is the invariant) |
| 2 | Point-in-time join spot-check | `Gold` → "Spot-check point-in-time joins" | `plan_code_at_admission` differs from `plan_code_today` for members whose plan changed — proving the as-of join works |
| 3 | Out-of-network check | `Gold` (last cell) | claims with `is_network_at_admission = false` exist and are consistent with `hospital_network_history` |
| 4 | Quarantine honesty | any SQL client | `select count(*), reject_reasons from silver.claims_quarantine group by all` — every rejected row is accounted for here, and `claims_clean + quarantine = deduped bronze` |
| 5 | Expectation metrics | pipeline UI → Data quality | every rule shows pass counts; failures land in quarantine, not in the void |
| 6 | SCD2 integrity | SQL | each member has at most one `is_current` row; no overlapping `effective_from/effective_till` intervals |

### 8.3 Delta ops checks

`src/Delta ops` demonstrates the operational layer: `DESCRIBE HISTORY silver.dim_policy_scd2` (audit trail of every MERGE), time travel (`VERSION AS OF 1`), liquid clustering (`ALTER TABLE gold.claims_enriched CLUSTER BY (admission_month, hospital_id)`) and `OPTIMIZE`.

### 8.4 The pytest harness (`tests/`)

The project ships a Python test harness wired to the workspace via **Databricks Connect**:

* `tests/conftest.py` builds a SparkSession (falls back to serverless automatically when no compute is configured) and provides a `load_fixture` fixture that reads JSON/CSV from `fixtures/`.
* `tests/sample_taxis_test.py` is the **template sample** from project init — it does not test this project. Replace it with real tests (e.g. reconciliation checks against `gold.claims_enriched`) as you go.

Run the harness from the project root with [uv](https://docs.astral.sh/uv):

```bash
cd Saffron_Shield
uv sync          # install dev deps (pytest, databricks-connect, ruff)
uv run pytest    # run the test suite
uv run ruff check .
```

## 9. Governance

`src/governance` implements three governance requirements. All objects live in the `claims.gov` schema.

### R-1 Access control

* **Column masks** — function `gov.mask_last4(v)` shows the full value to members of `gov.admin_users`, everyone else sees `****1234`. Applied to `raw.members.national_id` and `raw.members.phone`.
* **Row filter** — function `gov.claims_state_filter(hospital_id)` on `gold.claims_enriched`: admins see everything; other users see only claims for hospitals in states listed for them in `gov.user_state`.
* **Proof tables** — `gov.proof_member_masked` (masked view of members), `gov.proof_member_admin` (unmasked, after adding yourself to `admin_users`), `gov.proof_member_state` (claim→state mapping behind the row filter).

### R-2 Discovery & lineage

* PII column tags (`pii = true`) on `raw.members.name`, `dob`, `national_id`, `phone`, `email`.
* Full `COMMENT ON TABLE` / column comments for the gold tables — the catalog is self-describing in Catalog Explorer, and lineage for every table is viewable there.

### Audit

`gov.audit_claims_enriched` snapshots `DESCRIBE HISTORY gold.claims_enriched` (version, timestamp, user, operation) — a queryable change log of who changed the gold layer and how.

## 10. Repository layout

```
Saffron_Shield/
├── databricks.yml                        # bundle: name, variables, dev/prod targets
├── AGENTS.md                              # instructions for AI coding agents
├── pyproject.toml                        # uv/pytest project (dev deps, ruff)
├── .gitignore
├── .vscode/                               # editor config
├── tests/                                 # pytest harness (Databricks Connect)
│   ├── conftest.py                        # spark fixture + fixtures/ loader
│   └── sample_taxis_test.py               # template sample — replace with real tests
├── fixtures/                              # test data for the loader above
├── resources/
│   └── saffron_shield_daily_pipleine.job.yml   # the daily job (tasks + file-arrival trigger)
└── src/
    ├── README.md                          # ← this file
    ├── 00_setup                           # catalog/schemas/volume + reference tables
    ├── 01_bronze                          # Auto Loader: claims_raw/ → bronze.claims
    ├── claims_pipeline/
    │   ├── transformations/my_transformation.py   # SDP code: claims_clean, claims_quarantine
    │   └── explorations/                  # pipeline experiment notebooks
    ├── policy                             # CDC → silver.dim_policy_scd2 (SCD2 MERGE)
    ├── Gold                               # gold.claims_enriched + hospital_monthly + tests
    ├── monthly_hospital_summary.designer.ipynb   # Visual data prep (task 'Designer')
    ├── governance                         # masks, row filters, tags, comments, audit
    └── Delta ops                          # time travel, history, clustering, optimize
```

## 11. Conventions & gotchas

* New SQL cells in these notebooks default to Python — start SQL cells with `%sql` (or `--`) so they are parsed as SQL.
* A notebook cell with multiple SQL statements displays only the **last** statement's result.
* `01_bronze`, `policy` and `Gold` all take a `catalog` widget/parameter (default `claims`); the job passes it from the bundle's `catalog` variable. Change the target variable, not the notebooks, to redirect environments.
* Reference files in `raw.*` are built with `create table` (not `if not exists`) in two cases (`plans`, `hospitals`) — re-running those cells drops and rebuilds them.
* The file-arrival trigger is **unpaused** in the job definition, so dev deploys will also fire on new files. Pause it via `pause_status: PAUSED` if you don't want that.
