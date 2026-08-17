"""Pure validation and splitting for v1 assignment scope manifests."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any


PRIMARY_PHASE = "primary"
SECONDARY_PHASE = "secondary"
EXPECTED_PRIMARY_SCOPE_COUNT = 77
EXPECTED_SECONDARY_SCOPE_COUNT = 2
ALLOWED_MANIFEST_FIELDS = frozenset(
    {"scope", "phase", "inherit_basic_from"}
)
V1_SECONDARY_INHERITANCE = (
    ("SB2", "SB1"),
    ("OC2", "OC1"),
)
V1_CONFIGURED_SCOPE_EXCLUSIONS = frozenset({"HN1"})


@dataclass(frozen=True)
class SplitAssignmentScopeManifest:
    """Validated inputs for the primary and secondary task loops."""

    primary: tuple[dict[str, str], ...]
    secondary: tuple[dict[str, str], ...]

    @property
    def primary_json(self) -> str:
        return json.dumps(self.primary, separators=(",", ":"))

    @property
    def secondary_json(self) -> str:
        return json.dumps(self.secondary, separators=(",", ":"))


def _require_text(value: Any, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a non-empty string")
    return value.strip()


def split_assignment_scope_manifest(
    raw_manifest: Any,
) -> SplitAssignmentScopeManifest:
    """Validate one v1 manifest and split it without changing scope order."""
    manifest_json = _require_text(
        raw_manifest,
        label="scope_manifest_json",
    )
    try:
        parsed = json.loads(manifest_json)
    except json.JSONDecodeError as exc:
        raise ValueError(
            "scope_manifest_json must contain valid JSON"
        ) from exc

    if not isinstance(parsed, list):
        raise ValueError("scope_manifest_json must be a JSON list")

    primary: list[dict[str, str]] = []
    secondary: list[dict[str, str]] = []
    seen_scopes: set[str] = set()

    for index, raw_entry in enumerate(parsed):
        label = f"scope_manifest_json entry {index}"
        if not isinstance(raw_entry, dict):
            raise ValueError(f"{label} must be a JSON object")

        unexpected_fields = sorted(
            set(raw_entry) - ALLOWED_MANIFEST_FIELDS
        )
        if unexpected_fields:
            raise ValueError(
                f"{label} contains unsupported fields: "
                + ", ".join(unexpected_fields)
            )

        scope = _require_text(
            raw_entry.get("scope"),
            label=f"{label}.scope",
        )
        if scope in seen_scopes:
            raise ValueError(
                f"scope_manifest_json contains duplicate scope {scope!r}"
            )
        seen_scopes.add(scope)

        phase = _require_text(
            raw_entry.get("phase"),
            label=f"{label}.phase",
        )
        inherit_basic_from = raw_entry.get("inherit_basic_from")

        if phase == PRIMARY_PHASE:
            if inherit_basic_from is not None:
                raise ValueError(
                    f"{label}.inherit_basic_from is only valid for "
                    "secondary scopes"
                )
            if secondary:
                raise ValueError(
                    "primary scopes must precede secondary scopes"
                )
            primary.append({"scope": scope, "phase": PRIMARY_PHASE})
            continue

        if phase != SECONDARY_PHASE:
            raise ValueError(
                f"{label}.phase must be {PRIMARY_PHASE!r} or "
                f"{SECONDARY_PHASE!r}"
            )

        inherit_scope = _require_text(
            inherit_basic_from,
            label=f"{label}.inherit_basic_from",
        )
        if inherit_scope not in {entry["scope"] for entry in primary}:
            raise ValueError(
                f"{label}.inherit_basic_from must reference an earlier "
                "primary scope"
            )
        if inherit_scope == scope:
            raise ValueError(
                f"{label}.inherit_basic_from cannot reference itself"
            )
        secondary.append(
            {
                "scope": scope,
                "phase": SECONDARY_PHASE,
                "inherit_basic_from": inherit_scope,
            }
        )

    if len(primary) != EXPECTED_PRIMARY_SCOPE_COUNT:
        raise ValueError(
            "scope_manifest_json must contain exactly "
            f"{EXPECTED_PRIMARY_SCOPE_COUNT} primary scopes"
        )
    if len(secondary) != EXPECTED_SECONDARY_SCOPE_COUNT:
        raise ValueError(
            "scope_manifest_json must contain exactly "
            f"{EXPECTED_SECONDARY_SCOPE_COUNT} secondary scopes"
        )

    secondary_inheritance = {
        entry["scope"]: entry["inherit_basic_from"]
        for entry in secondary
    }
    if secondary_inheritance != dict(V1_SECONDARY_INHERITANCE):
        expected = ", ".join(
            f"{scope}->{inherit_scope}"
            for scope, inherit_scope in V1_SECONDARY_INHERITANCE
        )
        raise ValueError(
            "scope_manifest_json secondary scopes must be exactly "
            + expected
        )

    return SplitAssignmentScopeManifest(
        primary=tuple(primary),
        secondary=tuple(secondary),
    )


def _entry_scope(entry: Any, *, index: int) -> str:
    if isinstance(entry, Mapping):
        value = entry.get("scope")
    else:
        value = getattr(entry, "scope", None)
    return _require_text(value, label=f"scope manifest entry {index}.scope")


def validate_configured_v1_scope_manifest(
    scope_manifest: Iterable[Any],
    configured_locations: Mapping[str, Any] | Iterable[str],
) -> None:
    """Require manifest scopes to match configured v1 locations except HN1."""
    if isinstance(configured_locations, Mapping):
        configured_scopes = tuple(configured_locations.keys())
    else:
        configured_scopes = tuple(configured_locations)

    actual_scopes = tuple(
        _entry_scope(entry, index=index)
        for index, entry in enumerate(scope_manifest)
    )
    if len(set(actual_scopes)) != len(actual_scopes):
        raise ValueError("v1 scope manifest contains duplicate scopes")

    expected_scopes = set(configured_scopes).difference(
        V1_CONFIGURED_SCOPE_EXCLUSIONS
    )
    actual_scope_set = set(actual_scopes)
    if actual_scope_set != expected_scopes:
        missing = sorted(expected_scopes - actual_scope_set)
        unexpected = sorted(actual_scope_set - expected_scopes)
        details = []
        if missing:
            details.append("missing: " + ", ".join(missing))
        if unexpected:
            details.append("unexpected: " + ", ".join(unexpected))
        raise ValueError(
            "v1 scope manifest must match configured locations excluding HN1"
            + (f" ({'; '.join(details)})" if details else "")
        )
