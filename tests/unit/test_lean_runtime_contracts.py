import ast
from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _read(path: str) -> str:
    return (PROJECT_ROOT / path).read_text()


def _job(path: str, key: str) -> dict:
    resource = yaml.safe_load(_read(path))
    anchor = next(
        value for name, value in resource.items() if "_config" in name
    )
    return anchor[key]


def _function_source(path: str, function: str) -> str:
    source = _read(path)
    tree = ast.parse(source)
    node = next(
        item
        for item in ast.walk(tree)
        if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
        and item.name == function
    )
    return ast.get_source_segment(source, node)


def test_large_outputs_remain_distributed_spark_writes():
    bulk_job = _read("jobs/nextads_assignment/bulk_build.py")
    runtime = _read("src/next_ads/common/spark_runtime.py")
    prediction = _function_source(
        "src/next_ads/ranking/theme_affinity/predict.py",
        "build_predictions",
    )
    publisher = _function_source(
        "src/next_ads/decisioning/assignment_publication.py",
        "publish_bulk_assignment_build",
    )

    for source in (bulk_job, publisher):
        assert "coalesce(1)" not in source
        assert ".toPandas(" not in source
        assert ".collectAsList(" not in source
    assert "configure_lean_spark(spark)" in bulk_job
    assert '"spark.sql.adaptive.enabled": "true"' in runtime
    assert '"spark.sql.shuffle.partitions": "auto"' in runtime
    assert '"spark.sql.adaptive.skewJoin.enabled": "true"' in runtime
    assert '"spark.sql.execution.arrow.maxRecordsPerBatch": "10000"' in runtime
    assert "PREDICTION_PARTITIONS" in prediction
    assert 'spark.conf.get("spark.default.parallelism"' not in prediction
    assert "StorageLevel.MEMORY_AND_DISK" in bulk_job
    assert "target_partitions = 2048" in publisher
    assert "else 512" in publisher
    assert ".repartition(" in publisher
    assert "StorageLevel.DISK_ONLY" in publisher


def test_bulk_assignment_has_one_graph_and_one_publisher_per_route():
    entrypoint = _read("jobs/nextads_assignment/bulk_build.py")
    bulk_graph = _read("src/next_ads/decisioning/bulk_assignment.py")

    assert "runpy" not in entrypoint
    assert "stage_assignment_scope(" not in entrypoint
    assert entrypoint.count("publish_bulk_assignment_build(") == 1
    assert "build_v1_assignments(" in entrypoint
    assert "build_v2_assignments(" in entrypoint
    assert "for entry in scope_manifest" in bulk_graph
    assert "replace_scope_by_name(" not in bulk_graph
    assert "replace_table_by_name(" not in bulk_graph


def test_critical_publishers_do_not_restore_full_frame_audits():
    publisher_functions = (
        (
            "src/next_ads/ranking/foundation_publication.py",
            "publish_required_foundation_outputs",
        ),
        (
            "src/next_ads/ranking/provider_publication.py",
            "stage_provider_signals",
        ),
        (
            "src/next_ads/candidates/publication.py",
            "finalize",
        ),
        (
            "src/next_ads/decisioning/assignment_publication.py",
            "publish_bulk_assignment_build",
        ),
    )
    for path, function in publisher_functions:
        source = _function_source(path, function)
        assert "countDistinct" not in source
        assert "to_json" not in source
        assert "coalesce(1)" not in source


def test_same_provider_is_expanded_without_duplicating_its_spark_graph():
    source = _function_source(
        "src/next_ads/candidates/publication.py",
        "publish_provider",
    )

    assert "crossJoin(F.broadcast(entry_frame))" in source
    assert source.count("self._score_frames.append(") == 1
    assert "spark.createDataFrame" not in source


def test_scoring_job_graphs_keep_modularity_without_extra_clusters():
    theme = _job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_theme_affinity.yml",
        "mktg_next_uk_nextads_theme_affinity_cicd",
    )
    markov = _job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_markov_scoring.yml",
        "mktg_next_uk_nextads_markov_scoring_cicd",
    )

    assert [task["task_key"] for task in theme["tasks"]] == [
        "prepare_foundation_context",
        "predict_data_prep",
        "publish_and_score",
    ]
    assert [task["task_key"] for task in markov["tasks"]] == [
        "build_and_publish_markov"
    ]
    assert "saveAsTable(" not in _read(
        "jobs/nextads_candidates/build_theme_scores.py"
    )
    theme_entrypoint = _read("jobs/orchestration/publish_theme_affinity.py")
    markov_entrypoint = _read("jobs/nextads_candidates/build_theme_scores.py")
    for source in (theme_entrypoint, markov_entrypoint):
        assert 'status="CONSUMED"' in source or '"CONSUMED"' in source
        assert 'status="FAILED"' in source or '"FAILED"' in source


def test_theme_affinity_repair_checks_receipt_before_any_spark_rebuild():
    source = _function_source(
        "jobs/orchestration/publish_theme_affinity.py",
        "main",
    )

    receipt_lookup = source.index("find_delta_write_receipt(")
    prediction = source.index("build_predictions(")
    staging = source.index("stage_model_output(")
    assert receipt_lookup < prediction
    assert receipt_lookup < staging
    assert "REUSE_INCOMPLETE_ATTEMPT=True" in source
    assert "ACTIVATE_CONTEXT=True" in source


def test_markov_repair_checks_receipt_before_loading_large_input_tables():
    source = _function_source(
        "jobs/nextads_candidates/build_theme_scores.py",
        "_run_markov",
    )

    receipt_lookup = source.index("find_delta_write_receipt(")
    first_large_table = source.index('PRODUCT_CATALOG = cfg["tables"]')
    assert receipt_lookup < first_large_table
    assert "REUSE_INCOMPLETE_ATTEMPT=True" in source


def test_theme_affinity_contracts_fail_before_lakeflow_starts():
    prepare = _function_source(
        "jobs/orchestration/prepare_scoring_foundation_context.py",
        "main",
    )
    marker = _function_source(
        "src/next_ads/ranking/theme_affinity/dlt_pipeline.py",
        "build_marker",
    )

    assert "validate_foundation_output_manifest_contract(" in prepare
    assert "validate_provider_publication_contract(" in prepare
    assert "missing_targets" in prepare
    assert "incompatible_existing_outputs" in prepare
    assert "schema=" in marker


def test_page_routes_use_one_fixed_four_worker_photon_task():
    routes = (
        (
            "pipelines/databricks/jobs/mktg_next_uk_nextads_page_build.yml",
            "mktg_next_uk_nextads_page_build_cicd",
            "build_and_publish_v1",
        ),
        (
            "pipelines/databricks/jobs/mktg_next_uk_nextads_page_build_v2.yml",
            "mktg_next_uk_nextads_page_build_cicd_v2",
            "build_and_publish_v2",
        ),
    )
    for path, key, task_key in routes:
        job = _job(path, key)
        build_tasks = [
            task for task in job["tasks"] if task["task_key"] == task_key
        ]
        assert len(build_tasks) == 1
        assert build_tasks[0]["job_cluster_key"] == (
            "next_ads_job_cluster_D32ads_v5_4_4_photon"
        )
        assert build_tasks[0]["spark_python_task"]["python_file"].endswith(
            "jobs/nextads_assignment/bulk_build.py"
        )

    clusters = _read("pipelines/databricks/variables/clusters.yml")
    cluster = clusters.split(
        "job_cluster_key: next_ads_job_cluster_D32ads_v5_4_4_photon",
        1,
    )[1].split("- job_cluster_key:", 1)[0]
    assert "min_workers: 4" in cluster
    assert "max_workers: 4" in cluster


def test_compatibility_runs_asynchronously_from_ready_versions():
    provider = _job(
        "pipelines/databricks/jobs/"
        "mktg_next_uk_nextads_theme_feature_compatibility.yml",
        "mktg_next_uk_nextads_theme_feature_compatibility_cicd",
    )
    candidate = _job(
        "pipelines/databricks/jobs/"
        "mktg_next_uk_nextads_candidate_compatibility.yml",
        "mktg_next_uk_nextads_candidate_compatibility_cicd",
    )

    assert provider["schedule"]["quartz_cron_expression"] == "0 0 17 * * ?"
    assert candidate["schedule"]["quartz_cron_expression"] == "0 0 21 * * ?"
    assert "publish_provider_compatibility.py" in _read(
        "pipelines/databricks/jobs/"
        "mktg_next_uk_nextads_theme_feature_compatibility.yml"
    )
    assert "publish_candidate_compatibility.py" in _read(
        "pipelines/databricks/jobs/"
        "mktg_next_uk_nextads_candidate_compatibility.yml"
    )
