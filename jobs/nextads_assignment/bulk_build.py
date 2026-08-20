"""Build and atomically publish every scope for one assignment route."""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
except NameError:
    notebook_path = (
        dbutils.notebook.entry_point.getDbutils()  # type: ignore[name-defined]
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
    sys.path.insert(0, str(SRC_ROOT))
    sys.path.insert(1, str(PROJECT_ROOT))

from pyspark import StorageLevel

from dsutils.argparser import get_job_parser
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger

from jobs.nextads_assignment.publish_build import (
    build_assignment_scope_contract,
    parse_scope_manifest_json,
    resolve_assignment_tables,
    validate_configured_scope_manifest,
)
from next_ads.common import config_manager, etl
from next_ads.common.paths import load_client_config
from next_ads.common.spark_runtime import configure_lean_spark
from next_ads.decisioning.assignment_publication import (
    AssignmentColumnContract,
    publish_bulk_assignment_build,
)
from next_ads.decisioning.bulk_assignment import (
    build_v1_assignments,
    build_v2_assignments,
)
from next_ads.decisioning.candidate_inputs import (
    load_accepted_candidate_inputs,
)
from next_ads.ranking.scoring_inputs import read_delta_version


def _required(parser, name: str) -> str:
    value = parser.get_arg(name)
    if value is None or not str(value).strip():
        raise ValueError(f"{name} must be provided")
    return str(value).strip()


def _integer(parser, name: str, *, minimum: int) -> int:
    raw = _required(parser, name)
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    return value


def main() -> None:
    parser = get_job_parser()
    parser._parse_args()
    job_env = _required(parser, "--job_env")
    client = _required(parser, "--client")
    route = _required(parser, "--route").lower()
    if route not in {"v1", "v2"}:
        raise ValueError("--route must be one of: v1, v2")
    log_level = parser.get_arg("--log_level")
    configure_logging(
        log_level=log_level
    ) if log_level else configure_logging()
    logger = get_logger(__name__)
    try:
        run_date = date.fromisoformat(_required(parser, "--run_date"))
    except ValueError as exc:
        raise ValueError("--run_date must use ISO format YYYY-MM-DD") from exc
    build_run_id = _required(parser, "--build_run_id")
    candidate_attempt = _required(parser, "--candidate_build_attempt_id")
    task_run_id = _integer(parser, "--task_run_id", minimum=1)
    execution_count = _integer(parser, "--execution_count", minimum=0)
    cells_table = _required(parser, "--customer_cells_table")
    cells_version = _integer(
        parser, "--customer_cells_delta_version", minimum=0
    )
    git_commit = _required(parser, "--git_commit")
    manifest = parse_scope_manifest_json(
        _required(parser, "--scope_manifest_json")
    )

    spark = configure_spark()
    configure_lean_spark(spark)
    config = config_manager.load_config(job_env, client=client)
    cfg = load_client_config(client)
    validate_configured_scope_manifest(config, route, manifest)
    scope_contract = build_assignment_scope_contract(route, manifest)
    tables = resolve_assignment_tables(config, route)

    cells = (
        read_delta_version(spark, cells_table, cells_version)
        .drop("rundate")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    candidates = load_accepted_candidate_inputs(
        spark,
        builds_table=config.tables_write.candidate_builds,
        scores_table=config.tables_write.candidate_scores,
        ad_sets_table=config.tables_write.candidate_ad_sets,
        candidate_build_attempt_id=candidate_attempt,
        route=route,
    )
    nextgen_table = cfg["tables"]["read"]["nextgenads_assignments_latest"]
    logger.info(
        "Building %s assignments as one graph across %s scopes",
        route,
        len(manifest),
    )
    if route == "v1":
        control = spark.table(
            config.tables_write.control_sheet_latest
        ).persist(StorageLevel.MEMORY_AND_DISK)
        results_table = etl.map_tbl(
            cfg["tables"]["write"]["results_ads"],
            catalog=config.catalog_read,
            schema=config.schema_read,
            client=client,
        )
        assignments = build_v1_assignments(
            spark,
            cfg=cfg,
            scope_manifest=manifest,
            control=control,
            customer_cells=cells,
            candidate_inputs=candidates,
            nextgen_assignments_table=nextgen_table,
            results=spark.table(results_table),
            run_date=run_date,
        )
    else:
        control = spark.table(
            config.tables_write.control_sheet_latest_v2
        ).persist(StorageLevel.MEMORY_AND_DISK)
        assignments = build_v2_assignments(
            spark,
            cfg=cfg,
            page_types=tuple(entry.scope for entry in manifest),
            control=control,
            customer_cells=cells,
            candidate_inputs=candidates,
            nextgen_assignments_table=nextgen_table,
        )

    logger.info(
        "Validating the final %s key once, then publishing history and latest",
        route,
    )
    publish_bulk_assignment_build(
        spark,
        assignments,
        tables=tables,
        columns=AssignmentColumnContract(),
        scope_contract=scope_contract,
        build_run_id=build_run_id,
        build_date=run_date,
        task_run_id=task_run_id,
        execution_count=execution_count,
        provenance=candidates.provenance,
        git_commit=git_commit,
    )


if __name__ == "__main__":
    main()


__all__ = ["main"]
