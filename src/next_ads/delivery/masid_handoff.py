from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

from pyspark.sql import functions as F

from next_ads.utils import etl


REQUIRED_ASSIGNMENT_COLUMNS = frozenset(
    {"AccountNumber", "Location", "MASID", "rundate"}
)


@dataclass(frozen=True)
class MasidHandoffSummary:
    table_name: str
    columns: set[str]
    row_count: int
    rundates: list[str]
    null_masid_count: int
    location_count: int


def expected_rundate(value: str | None) -> str:
    if value:
        datetime.strptime(value, "%Y-%m-%d")
        return value
    return datetime.now(ZoneInfo("Europe/London")).date().isoformat()


def resolve_assignments_latest_table(config, client_config: dict, client: str) -> str:
    tbl_args = {
        "catalog": config.catalog_write,
        "schema": config.schema_write,
        "client": client,
    }
    return etl.map_tbl(
        client_config["tables"]["write"]["assignments_latest"], **tbl_args
    )


def summarise_masid_handoff_table(spark, assignments_latest: str) -> MasidHandoffSummary:
    df_assignments = spark.table(assignments_latest)
    columns = set(df_assignments.columns)

    if not REQUIRED_ASSIGNMENT_COLUMNS.issubset(columns):
        return MasidHandoffSummary(
            table_name=assignments_latest,
            columns=columns,
            row_count=0,
            rundates=[],
            null_masid_count=0,
            location_count=0,
        )

    return MasidHandoffSummary(
        table_name=assignments_latest,
        columns=columns,
        row_count=df_assignments.count(),
        rundates=[
            str(row["rundate"])
            for row in df_assignments.select(F.to_date("rundate").alias("rundate"))
            .distinct()
            .collect()
        ],
        null_masid_count=df_assignments.where(F.col("MASID").isNull()).count(),
        location_count=df_assignments.select("Location").distinct().count(),
    )


def validate_masid_handoff_summary(
    summary: MasidHandoffSummary, expected_run_date: str
) -> None:
    missing_columns = REQUIRED_ASSIGNMENT_COLUMNS.difference(summary.columns)
    if missing_columns:
        raise AssertionError(
            "MASID handoff table is missing required columns: "
            + ", ".join(sorted(missing_columns))
        )

    if summary.row_count == 0:
        raise AssertionError(f"{summary.table_name} is empty")

    if summary.rundates != [expected_run_date]:
        raise AssertionError(
            f"{summary.table_name} rundates {summary.rundates} do not match "
            f"expected handoff date {expected_run_date}"
        )

    if summary.null_masid_count > 0:
        raise AssertionError(
            f"{summary.table_name} contains {summary.null_masid_count:,} "
            "null MASID values"
        )

    if summary.location_count == 0:
        raise AssertionError(
            f"{summary.table_name} contains no assignment locations"
        )


def check_masid_handoff_table(
    spark,
    assignments_latest: str,
    expected_run_date: str,
    logger=None,
) -> MasidHandoffSummary:
    summary = summarise_masid_handoff_table(spark, assignments_latest)

    if logger:
        logger.info(f"Found assignment rundates: {summary.rundates}")

    validate_masid_handoff_summary(summary, expected_run_date)

    if logger:
        logger.info(
            "MASID handoff check passed for "
            f"{summary.table_name}: {summary.row_count:,} rows, "
            f"{summary.location_count:,} locations, rundate {expected_run_date}"
        )

    return summary
