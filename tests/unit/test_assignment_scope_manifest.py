import json
from types import SimpleNamespace

import pytest

from jobs.nextads_assignment.prepare_scope_manifest import (
    set_assignment_scope_task_values,
)
from next_ads.decisioning.assignment_manifest import (
    split_assignment_scope_manifest,
    validate_configured_v1_scope_manifest,
)


def _manifest() -> list[dict[str, str]]:
    primary_scopes = [
        "SB1",
        "OC1",
        *[f"P{index:02}" for index in range(1, 76)],
    ]
    return [
        *[
            {"scope": scope, "phase": "primary"}
            for scope in primary_scopes
        ],
        {
            "scope": "SB2",
            "phase": "secondary",
            "inherit_basic_from": "SB1",
        },
        {
            "scope": "OC2",
            "phase": "secondary",
            "inherit_basic_from": "OC1",
        },
    ]


def test_split_manifest_preserves_scope_order_and_secondary_inheritance():
    result = split_assignment_scope_manifest(json.dumps(_manifest()))

    primary = list(result.primary)
    secondary = list(result.secondary)

    assert [entry["scope"] for entry in primary] == [
        "SB1",
        "OC1",
        *[f"P{index:02}" for index in range(1, 76)],
    ]
    assert secondary == [
        {
            "scope": "SB2",
            "phase": "secondary",
            "inherit_basic_from": "SB1",
        },
        {
            "scope": "OC2",
            "phase": "secondary",
            "inherit_basic_from": "OC1",
        },
    ]


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda entries: entries[:-1],
            "exactly 2 secondary scopes",
        ),
        (
            lambda entries: [
                *entries,
                {"scope": "P01", "phase": "primary"},
            ],
            "duplicate scope",
        ),
        (
            lambda entries: [
                *entries[:-2],
                {
                    "scope": "SB2",
                    "phase": "secondary",
                    "inherit_basic_from": "UNKNOWN",
                },
                entries[-1],
            ],
            "must reference an earlier primary scope",
        ),
        (
            lambda entries: [
                *entries[:-2],
                {
                    "scope": "SB2",
                    "phase": "primary",
                    "inherit_basic_from": "SB1",
                },
                entries[-1],
            ],
            "only valid for secondary scopes",
        ),
    ],
)
def test_split_manifest_rejects_invalid_complete_builds(mutate, message):
    with pytest.raises(ValueError, match=message):
        split_assignment_scope_manifest(json.dumps(mutate(_manifest())))


def test_notebook_entrypoint_sets_both_scope_task_values():
    recorded: dict[str, list[dict[str, str]]] = {}
    dbutils_obj = SimpleNamespace(
        widgets=SimpleNamespace(
            get=lambda name: (
                json.dumps(_manifest())
                if name == "scope_manifest_json"
                else None
            )
        ),
        jobs=SimpleNamespace(
            taskValues=SimpleNamespace(
                set=lambda *, key, value: recorded.__setitem__(key, value)
            )
        ),
    )

    set_assignment_scope_task_values(dbutils_obj)

    assert list(recorded) == [
        "primary_scope_manifest",
        "secondary_scope_manifest",
    ]
    assert len(recorded["primary_scope_manifest"]) == 77
    assert len(recorded["secondary_scope_manifest"]) == 2


def test_split_manifest_requires_exact_secondary_scope_mappings():
    manifest = _manifest()
    manifest[-2]["inherit_basic_from"] = "P03"

    with pytest.raises(
        ValueError,
        match="secondary scopes must be exactly SB2->SB1, OC2->OC1",
    ):
        split_assignment_scope_manifest(json.dumps(manifest))


def test_configured_v1_scopes_allow_only_explicit_hn1_exclusion():
    manifest = _manifest()
    configured_locations = {
        **{entry["scope"]: {} for entry in manifest},
        "HN1": {},
    }

    validate_configured_v1_scope_manifest(
        manifest,
        configured_locations,
    )


def test_configured_v1_scopes_reject_missing_or_unexpected_scope():
    manifest = _manifest()
    configured_locations = {
        **{entry["scope"]: {} for entry in manifest},
        "HN1": {},
        "EXPECTED": {},
    }

    with pytest.raises(
        ValueError,
        match=r"missing: EXPECTED",
    ):
        validate_configured_v1_scope_manifest(
            manifest,
            configured_locations,
        )
