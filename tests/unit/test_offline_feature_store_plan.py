import ast
import json
from pathlib import Path

import pytest
import yaml

from jobs.features.nextads import plan_offline_feature_store
from next_ads.features.offline_feature_store_plan import (
    DEFAULT_JOB_DEFINITION_PATH,
    build_offline_feature_store_plan,
    render_offline_feature_store_plan,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
REGISTRY_PATH = (
    PROJECT_ROOT / "configs" / "features" / "nextads_feature_store.yaml"
)


def _write_yaml(tmp_path, file_name: str, payload: dict) -> Path:
    path = tmp_path / file_name
    path.write_text(yaml.safe_dump(payload, sort_keys=False))
    return path


def test_offline_plan_resolves_the_same_logical_graph_for_every_environment():
    plan = build_offline_feature_store_plan()

    assert plan["mode"] == "READ_ONLY"
    assert plan["summary"] == {
        "ACTIVE": 17,
        "COMPATIBILITY": 3,
        "SCAFFOLD": 0,
    }
    environments = {
        environment["environment"]: environment
        for environment in plan["environments"]
    }
    assert {
        environment: (
            details["catalog"],
            details["schema"],
            details["repository_state"],
        )
        for environment, details in environments.items()
    } == {
        "DEV": (
            "marketingdata_dev",
            "nextads_feature_store",
            "REPO_DECLARED",
        ),
        "PREPROD": ("marketingdata_prod", "ds_sandbox", "PLANNED"),
        "PROD": ("marketingdata_prod", "nextads_feature_store", "PLANNED"),
    }

    feature_ids_by_environment = [
        [feature["feature_id"] for feature in environment["features"]]
        for environment in plan["environments"]
    ]
    assert feature_ids_by_environment[1:] == [
        feature_ids_by_environment[0],
        feature_ids_by_environment[0],
    ]
    assert len(feature_ids_by_environment[0]) == 20
    preprod_first_feature = environments["PREPROD"]["features"][0]
    assert preprod_first_feature["location_state"] == "RELEASE_ID_REQUIRED"
    assert "{release_id}" in preprod_first_feature["table_location"]


def test_offline_plan_reports_builders_dependencies_and_missing_contracts():
    plan = build_offline_feature_store_plan(environments=("DEV",))
    features = {
        feature["feature_id"]: feature
        for feature in plan["environments"][0]["features"]
    }

    active = features["next_uk_nextads_fs_account_theme_affinity_daily"]
    assert active["state"] == "ACTIVE"
    assert active["implemented"] is True
    assert active["builder"] == "build_theme_affinity_features"
    assert active["task_dependencies"] == [
        "build_account_features",
        "build_advert_features",
    ]
    assert active["missing_contracts"] == []

    pctr = features["next_uk_nextads_fs_pctr_model_input"]
    assert pctr["state"] == "COMPATIBILITY"
    assert pctr["implemented"] is True
    assert pctr["builder"] == "build_pctr_affinity_features"
    assert pctr["task_dependencies"] == [
        "build_account_features",
        "build_advert_features",
    ]
    assert pctr["missing_contracts"] == []


def test_compatibility_view_plan_reports_contract_readiness_not_live_state():
    plan = build_offline_feature_store_plan(environments=("PROD",))
    views = {
        view["name"]: view
        for view in plan["environments"][0]["compatibility_views"]
    }

    assert views["next_uk_nextads_theme_affinity_features_latest"][
        "status"
    ] == ("CONTRACT_READY")
    assert (
        views["next_uk_nextads_theme_affinity_features_latest"][
            "missing_contracts"
        ]
        == []
    )
    assert views["next_uk_nextads_pctr_features_latest"]["status"] == (
        "CONTRACT_READY"
    )
    assert views["next_uk_nextads_pctr_features_latest"][
        "missing_contracts"
    ] == []


def test_plan_output_is_deterministic_and_json_serializable():
    first_plan = build_offline_feature_store_plan()
    second_plan = build_offline_feature_store_plan()

    assert first_plan == second_plan
    assert json.loads(json.dumps(first_plan, sort_keys=True)) == first_plan
    assert render_offline_feature_store_plan(first_plan) == (
        render_offline_feature_store_plan(second_plan)
    )


def test_plan_cli_can_select_one_environment_and_emit_json(capsys):
    exit_code = plan_offline_feature_store.main(
        [
            "--environment",
            "PREPROD",
            "--release-id",
            "release/2026.08.12",
            "--format",
            "json",
        ]
    )

    assert exit_code == 0
    output = json.loads(capsys.readouterr().out)
    assert [
        environment["environment"] for environment in output["environments"]
    ] == ["PREPROD"]
    assert output["release_id"].startswith("release_2026_08_12_")
    assert output["environments"][0]["repository_state"] == "PLANNED"
    assert (
        output["environments"][0]["features"][0]["location_state"]
        == "RESOLVED"
    )


def test_plan_cli_rejects_unknown_environments():
    with pytest.raises(SystemExit) as exc_info:
        plan_offline_feature_store.main(["--environment", "TEST"])

    assert exc_info.value.code == 2


def test_plan_modules_use_only_the_read_only_import_allowlist():
    sources = [
        PROJECT_ROOT
        / "src"
        / "next_ads"
        / "features"
        / "offline_feature_store_plan.py",
        PROJECT_ROOT
        / "jobs"
        / "features"
        / "nextads"
        / "plan_offline_feature_store.py",
    ]
    imported_modules = set()
    for source_path in sources:
        tree = ast.parse(source_path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported_modules.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported_modules.add(node.module)

    assert imported_modules <= {
        "__future__",
        "argparse",
        "collections",
        "json",
        "next_ads.features.feature_store_registry",
        "next_ads.features.offline_feature_store_plan",
        "pathlib",
        "sys",
        "typing",
        "yaml",
    }


def test_plan_does_not_call_mutating_path_methods(monkeypatch):
    def reject_mutation(*args, **kwargs):
        pytest.fail("The read-only plan attempted a filesystem mutation")

    for method_name in (
        "mkdir",
        "rename",
        "replace",
        "rmdir",
        "touch",
        "unlink",
        "write_bytes",
        "write_text",
    ):
        monkeypatch.setattr(Path, method_name, reject_mutation)

    plan = build_offline_feature_store_plan(environments="DEV")

    assert plan["mode"] == "READ_ONLY"


def test_plan_rejects_an_implemented_feature_without_a_known_builder(tmp_path):
    raw_registry = yaml.safe_load(REGISTRY_PATH.read_text())
    raw_registry["feature_store"]["physical_tables"][0]["source_job"] = (
        "missing_builder"
    )
    registry_path = _write_yaml(tmp_path, "feature_store.yaml", raw_registry)

    with pytest.raises(ValueError, match="references unknown builder"):
        build_offline_feature_store_plan(registry_path=registry_path)


def test_plan_cross_checks_repository_declared_bundle_targets(tmp_path):
    raw_registry = yaml.safe_load(REGISTRY_PATH.read_text())
    raw_registry["feature_store"]["store_bindings"][0][
        "repository_declared"
    ] = False
    registry_path = _write_yaml(tmp_path, "feature_store.yaml", raw_registry)

    with pytest.raises(ValueError, match="bundle target DEV_FEATURE_STORE"):
        build_offline_feature_store_plan(registry_path=registry_path)


def test_plan_rejects_unknown_task_dependencies(tmp_path):
    raw_job = yaml.safe_load(DEFAULT_JOB_DEFINITION_PATH.read_text())
    tasks = raw_job["nextads_feature_store_config"][
        "mktg_next_uk_nextads_feature_store"
    ]["tasks"]
    tasks[0]["depends_on"] = [{"task_key": "missing_task"}]
    job_path = _write_yaml(tmp_path, "feature_store_job.yml", raw_job)

    with pytest.raises(ValueError, match="unknown task dependencies"):
        build_offline_feature_store_plan(job_definition_path=job_path)


def test_plan_rejects_multi_task_cycles(tmp_path):
    raw_job = yaml.safe_load(DEFAULT_JOB_DEFINITION_PATH.read_text())
    tasks = raw_job["nextads_feature_store_config"][
        "mktg_next_uk_nextads_feature_store"
    ]["tasks"]
    tasks[0]["depends_on"] = [{"task_key": "preflight_feature_store_sources"}]
    job_path = _write_yaml(tmp_path, "feature_store_job.yml", raw_job)

    with pytest.raises(ValueError, match="task graph contains a cycle"):
        build_offline_feature_store_plan(job_definition_path=job_path)


def test_programmatic_environment_selection_is_unambiguous():
    plan = build_offline_feature_store_plan(environments="DEV")
    assert [
        environment["environment"] for environment in plan["environments"]
    ] == ["DEV"]

    with pytest.raises(ValueError, match="At least one"):
        build_offline_feature_store_plan(environments=())
    with pytest.raises(ValueError, match="Duplicate offline store"):
        build_offline_feature_store_plan(environments=("DEV", "dev"))


def test_preprod_release_ids_resolve_to_isolated_plan_locations():
    first = build_offline_feature_store_plan(
        environments="PREPROD", release_id="release/foo-bar"
    )
    second = build_offline_feature_store_plan(
        environments="PREPROD", release_id="release/foo_bar"
    )

    first_location = first["environments"][0]["features"][0]["table_location"]
    second_location = second["environments"][0]["features"][0][
        "table_location"
    ]
    assert first_location != second_location
    assert "release_foo_bar_" in first_location
    assert "release_foo_bar_" in second_location


def test_rendered_plan_does_not_claim_live_deployment_or_snapshot_readiness():
    rendered = render_offline_feature_store_plan(
        build_offline_feature_store_plan()
    )

    assert "DEPLOYED" not in rendered
    assert "operational=" not in rendered
    assert "VIEW READY" not in rendered
    assert "REPO_DECLARED" in rendered
    assert "VIEW CONTRACT_READY" in rendered
