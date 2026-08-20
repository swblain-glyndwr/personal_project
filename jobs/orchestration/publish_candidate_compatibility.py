"""Publish one legacy preranked table from an exact READY candidate build."""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
except NameError:
    from dsutils.dbc import get_dbutils

    notebook_path = (
        get_dbutils()
        .notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )
    if not notebook_path.startswith("/Workspace"):
        notebook_path = "/Workspace" + notebook_path
    PROJECT_ROOT = Path(notebook_path).parents[2]
finally:
    SRC_ROOT = PROJECT_ROOT / "src"
    if SRC_ROOT.exists():
        sys.path.insert(0, str(SRC_ROOT))
    sys.path.insert(1, str(PROJECT_ROOT))

from dsutils.argparser import get_job_parser
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging
from pyspark.sql import functions as F

from next_ads.common import config_manager
from next_ads.common.delta_writes import (
    replace_table_by_name,
    validate_unique_non_null_keys,
)
from next_ads.ranking.scoring_inputs import read_delta_version


def _binding(bindings: dict, name: str) -> tuple[str, int]:
    value = bindings.get(name)
    if not isinstance(value, dict):
        raise ValueError(f"Candidate build has no {name} output binding")
    table = value.get("table")
    version = value.get("delta_version")
    if not isinstance(table, str) or not table:
        raise ValueError(f"Candidate {name} binding has no table")
    if (
        isinstance(version, bool)
        or not isinstance(version, int)
        or version < 0
    ):
        raise ValueError(f"Candidate {name} binding has no Delta version")
    return table, version


def main(
    job_env: str,
    client: str,
    run_date: str,
    route: str,
    log_level: str | None,
) -> None:
    configure_logging(
        log_level=log_level
    ) if log_level else configure_logging()
    if route not in {"v1", "v2"}:
        raise ValueError("Candidate compatibility route must be v1 or v2")
    spark = configure_spark()
    config = config_manager.load_config(job_env, client=client)
    resolved_date = date.fromisoformat(run_date)
    rows = (
        spark.table(config.tables_write.candidate_builds)
        .where(F.col("RunDate") == resolved_date)
        .where(F.col("Route") == route)
        .where(F.col("Status") == "READY_FOR_NEXTADS")
        .orderBy(
            F.col("CompletedAt").desc(),
            F.col("ExecutionCount").desc(),
            F.col("TaskRunID").desc(),
        )
        .limit(1)
        .collect()
    )
    if len(rows) != 1:
        raise ValueError(
            f"No same-day READY candidate build exists for {route}"
        )
    build = rows[0]
    bindings = json.loads(build["OutputBindingsJSON"])
    scores_table, scores_version = _binding(bindings, "candidate_scores")
    ad_sets_table, ad_sets_version = _binding(bindings, "candidate_ad_sets")
    attempt_id = build["CandidateBuildAttemptID"]
    scores = (
        read_delta_version(spark, scores_table, scores_version)
        .where(F.col("CandidateBuildAttemptID") == attempt_id)
        .where(F.col("ServingSlot") == "best")
    )
    ad_sets = read_delta_version(spark, ad_sets_table, ad_sets_version).where(
        F.col("CandidateBuildAttemptID") == attempt_id
    )
    scope_column = "Location" if route == "v1" else "PageType"
    legacy = (
        scores.alias("scores")
        .join(
            ad_sets.alias("sets"),
            on=[
                "CandidateBuildID",
                "CandidateBuildAttemptID",
                "RunDate",
                "Route",
                "AdSetID",
                "UniqueAdID",
            ],
            how="inner",
        )
        .select(
            "AccountNumber",
            "UniqueAdID",
            F.col("sets.ScopeValue").alias(scope_column),
            "Score",
            "TriggerScore",
            "Rank",
            F.col("RunDate").alias("rundate"),
        )
    )
    target = (
        config.tables_write.preranked_ads_from_themes_latest
        if route == "v1"
        else config.tables_write.preranked_ads_from_themes_v2_latest
    )
    target_schema = {
        field.name: field.dataType for field in spark.table(target).schema
    }
    legacy = legacy.select(
        *[
            F.col(column).cast(target_schema[column]).alias(column)
            for column in spark.table(target).columns
        ]
    )
    validate_unique_non_null_keys(
        legacy,
        ("AccountNumber", "UniqueAdID", scope_column),
    )
    replace_table_by_name(
        legacy,
        target,
        legacy.columns,
        spark=spark,
        build_id=build["CandidateBuildID"],
        attempt_id=attempt_id,
        git_commit=build["GitCommit"],
    )


if __name__ == "__main__":
    parser = get_job_parser()
    parser._parse_args()
    main(
        parser.get_arg("--job_env"),
        parser.get_arg("--client") or "next_uk",
        parser.get_arg("--run_date"),
        parser.get_arg("--route"),
        parser.get_arg("--log_level"),
    )
