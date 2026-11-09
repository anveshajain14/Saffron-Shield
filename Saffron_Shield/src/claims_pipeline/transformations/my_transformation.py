import pyspark.pipelines as dp
from pyspark.sql import functions as F, Window


DATA_START_DATE = F.to_date(F.lit("2025-07-01"))
DATA_END_DATE = F.to_date(F.lit("2026-06-30"))

claims = spark.read.table("bronze.claims")
w = Window.partitionBy("claim_id").orderBy(F.col("submitted_ts").desc())
latest = claims.withColumn("rank", F.row_number().over(w)) \
    .filter(F.col("rank") == 1) \
        .drop("rank")

hospital = spark.read.table("raw.hospitals") \
        .select(F.col("hospital_id").alias("_valid_hospital_id"))
member = spark.read.table("raw.members") \
        .select(F.col("member_id").alias("_valid_member_id"))

# --- Single source of truth: every rule as a SQL string. ---
# Used two ways below: F.expr(...) builds the Column for reject_reasons/quarantine,
# and the raw dict feeds expect_all_or_drop for claims_clean's per-rule metrics.
VALIDATION_RULES = {
    "valid_admission_date": """
        admission_date IS NOT NULL
        AND admission_date <= discharge_date
        AND admission_date >= DATE('2025-07-01')
        AND admission_date <= DATE('2026-06-30')
    """,
    "valid_discharge_date": """
        discharge_date IS NOT NULL
        AND discharge_date >= DATE('2025-07-01')
        AND discharge_date <= DATE('2026-06-30')
    """,
    "valid_settled_ts": """

    settled_ts IS NULL
    OR (
        to_date(settled_ts) >= DATE('2025-07-01')
        AND to_date(settled_ts) <= DATE('2026-06-30')

        AND
         submitted_ts IS NOT NULL
        AND settled_ts >= submitted_ts
    )
""",
    "valid_submit_ts": """
        submitted_ts IS NOT NULL
        AND to_date(submitted_ts) >= DATE('2025-07-01')
        AND to_date(submitted_ts) <= DATE('2026-06-30')
    """,
    "valid_approved_amount": "coalesce(approved_amount, 0) <= billed_amount",
    "valid_billed_amount": "billed_amount IS NOT NULL AND billed_amount > 0",
    "valid_room_days": "room_days is not null and room_days = greatest(1,datediff(to_date(discharge_date), to_date(admission_date)))",
    "valid_claim_status": "status IN ('submitted', 'approved', 'rejected', 'settled')",
    "submitted_status_consistency": """
        status != 'submitted'
        OR (approved_amount IS NULL AND rejection_code IS NULL AND settled_ts IS NULL)
    """,
    "approved_status_consistency": """
        status != 'approved'
        OR (approved_amount IS NOT NULL AND rejection_code IS NULL AND settled_ts IS NULL)
    """,
    "rejected_status_consistency": """
        status != 'rejected'
        OR (rejection_code IS NOT NULL AND settled_ts IS NULL)
    """,
    "settled_status_consistency": """
        status != 'settled'
        OR (approved_amount IS NOT NULL AND settled_ts IS NOT NULL AND rejection_code IS NULL)
    """,
    "valid_member_id": "member_id IS NOT NULL AND _valid_member_id IS NOT NULL",
    "valid_hospital_id": "hospital_id IS NOT NULL AND _valid_hospital_id IS NOT NULL",

}

# Map each rule name to its failure tag, for reject_reasons.
REASON_TAGS = {
    "valid_admission_date": "invalid_admission_date",
    "valid_discharge_date": "invalid_discharge_date",
    "valid_settled_ts": "invalid_settled_ts",
    "valid_submit_ts": "invalid_submit_ts",
    "valid_approved_amount": "invalid_approved_amount",
    "valid_billed_amount": "invalid_billed_amount",
    "valid_room_days": "invalid_room_days",
    "valid_claim_status": "invalid_claim_status",
    "submitted_status_consistency": "submitted_status_inconsistency",
    "approved_status_consistency": "approved_status_inconsistency",
    "rejected_status_consistency": "rejected_status_inconsistency",
    "settled_status_consistency": "settled_status_inconsistency",
    "valid_member_id": "invalid_member_id",
    "valid_hospital_id": "invalid_hospital_id",
}

reject_reasons = F.array_compact(
    F.array(*[
        F.when(~F.expr(VALIDATION_RULES[rule]), F.lit(tag))
        for rule, tag in REASON_TAGS.items()
    ])
)


claims_std = latest.select(
            "claim_id",
            "member_id",
            "hospital_id",
            "claim_type",
            F.try_to_date(F.col("admission_date"), "yyyy-MM-dd").alias("admission_date"),
            "approved_amount",
            "billed_amount",
            "channel",
            "diagnosis_code",
            F.try_to_date(F.col("discharge_date"), "yyyy-MM-dd").alias("discharge_date"),
            "is_pre_existing",
            "policy_id",
            "pre_auth_id",
            "procedure_code",
            "rejection_code",
            "room_days",
            "room_rent_per_day",
            F.try_to_timestamp(F.col("settled_ts"), F.lit("yyyy-MM-dd'T'HH:mm:ss")).alias("settled_ts"),
            "status",
            F.try_to_timestamp(F.col("submitted_ts"), F.lit("yyyy-MM-dd'T'HH:mm:ss")).alias("submitted_ts"),
        ) \
        .join(hospital, latest.hospital_id == hospital._valid_hospital_id, "left") \
        .join(member, latest.member_id == member._valid_member_id, "left") \
        .withColumn("reject_reasons", reject_reasons)


@dp.table(
    name="claims_clean",
    comment="Claims that passed every validation rule.",
)
@dp.expect_all_or_drop(VALIDATION_RULES)
def silver_claims():
    return claims_std.drop("reject_reasons")


@dp.table(
    name="claims_quarantine",
    comment="Claims that failed one or more validation rules. Visible for human review, "
            "tagged with every rule each row broke - never silently dropped.",
)
def quarantine_claims():
    return (
        claims_std
        .filter(F.size(F.col("reject_reasons")) > 0)
    )