"""Hand-rolled data-quality framework.

Chosen over Great Expectations per the build guide's own stated allowance
(project plan Section 7: "a small hand-rolled PySpark DQ module if you'd
rather not take the GE dependency") -- same signal (named checks, real
failure_reason values, a quarantine-rate gate), no new dependency, no GE
API-version risk on serverless compute.
"""

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass
class DQCheck:
    name: str
    condition: str  # SQL boolean expression evaluated against the row; TRUE = passes


def apply_checks(df: DataFrame, checks: list[DQCheck]) -> tuple[DataFrame, DataFrame]:
    """Split df into (valid, invalid) rows. A row fails if ANY check's
    condition is false. invalid rows get a failure_reason listing every
    check that failed, not just the first -- seeing all reasons at once
    beats re-discovering them one gate at a time."""
    failed_flags = [F.when(~F.expr(c.condition), F.lit(c.name)) for c in checks]
    flagged = df.withColumn(
        "_failed_checks",
        F.array_except(F.array(*failed_flags), F.array(F.lit(None).cast("string"))),
    )
    valid = flagged.filter(F.size("_failed_checks") == 0).drop("_failed_checks")
    invalid = (
        flagged.filter(F.size("_failed_checks") > 0)
        .withColumn("failure_reason", F.array_join("_failed_checks", "; "))
        .drop("_failed_checks")
    )
    return valid, invalid


class DQGateFailure(Exception):
    """Raised to fail the job/task when a run's quarantine rate exceeds
    threshold -- fails the DAG rather than silently promoting a bad batch
    to Gold."""


def enforce_dq_gate(total_count: int, quarantined_count: int, threshold: float, context: str) -> None:
    rate = (quarantined_count / total_count) if total_count else 0.0
    if rate > threshold:
        raise DQGateFailure(
            f"{context}: quarantine rate {rate:.1%} exceeds threshold {threshold:.1%} "
            f"({quarantined_count}/{total_count} rows) -- failing before promotion to Gold."
        )
