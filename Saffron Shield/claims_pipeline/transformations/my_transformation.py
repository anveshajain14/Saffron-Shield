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
# Validation checks
valid_admission_date = (
        F.col("admission_date").isNotNull()
        & (F.col("admission_date") <= F.col("discharge_date"))
        & (F.col("admission_date") >= DATA_START_DATE)
        & (F.col("admission_date") <= DATA_END_DATE)
    )

valid_discharge_date = (
        F.col("discharge_date").isNotNull()
        & (F.col("discharge_date") >= DATA_START_DATE)
        & (F.col("discharge_date") <= DATA_END_DATE)
    )

valid_settled_ts = (
        (F.to_date("settled_ts") >= DATA_START_DATE) & (F.to_date("settled_ts") <= DATA_END_DATE)
    )

valid_submit_ts = (
        F.col("submitted_ts").isNotNull()
        & (F.to_date("submitted_ts") >= DATA_START_DATE)
        & (F.to_date("submitted_ts") <= DATA_END_DATE)
    )

valid_approved_amount = F.coalesce(F.col("approved_amount"), F.lit(0)) <= F.col("billed_amount")

valid_billed_amount = F.col("billed_amount").isNotNull() & (F.col("billed_amount") > 0)

valid_room_days = F.col("room_days") == F.datediff(
        F.to_date(F.col("discharge_date")), F.to_date(F.col("admission_date"))
    )

valid_claim_status = F.col("status").isin("submitted", "approved", "rejected", "settled")

submitted_status_consistency = (
        (F.col("status") != "submitted")
        | (F.col("approved_amount").isNull() & F.col("rejection_code").isNull() & F.col("settled_ts").isNull())
    )

approved_status_consistency = (
        (F.col("status") != "approved")
        | (F.col("approved_amount").isNotNull() & F.col("rejection_code").isNull() & F.col("settled_ts").isNull())
    )

rejected_status_consistency = (
        (F.col("status") != "rejected")
        | (F.col("rejection_code").isNotNull() & F.col("settled_ts").isNull())
    )

settled_status_consistency = (
        (F.col("status") != "settled")
        | (F.col("approved_amount").isNotNull() & F.col("settled_ts").isNotNull() & F.col("rejection_code").isNull())
    )

reject_reasons = F.array_compact(
        F.array(
            F.when(~valid_admission_date, F.lit("invalid_admission_date")),
            F.when(~valid_discharge_date, F.lit("invalid_discharge_date")),
            F.when(~valid_settled_ts, F.lit("invalid_settled_ts")),
            F.when(~valid_submit_ts, F.lit("invalid_submit_ts")),
            F.when(~valid_approved_amount, F.lit("invalid_approved_amount")),
            F.when(~valid_billed_amount, F.lit("invalid_billed_amount")),
            F.when(~valid_room_days, F.lit("invalid_room_days")),
            F.when(~valid_claim_status, F.lit("invalid_claim_status")),
            F.when(~submitted_status_consistency, F.lit("submitted_status_inconsistency")),
            F.when(~approved_status_consistency, F.lit("approved_status_inconsistency")),
            F.when(~rejected_status_consistency, F.lit("rejected_status_inconsistency")),
            F.when(~settled_status_consistency, F.lit("settled_status_inconsistency")),
            F.when(F.col("member_id").isNull() | F.col("_valid_member_id").isNull(), F.lit("invalid_member_id")),
            F.when(F.col("hospital_id").isNull() | F.col("_valid_hospital_id").isNull(), F.lit("invalid_hospital_id")),
        )
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
def silver_claims():
    return (
        claims_std
        .filter(F.size(F.col("reject_reasons")) == 0)
        .drop("reject_reasons")
    )

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