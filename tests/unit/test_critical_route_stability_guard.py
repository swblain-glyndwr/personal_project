import ast
import hashlib
import re
from collections import Counter
from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
AUDIT_PATH = (
    PROJECT_ROOT / "docs/architecture/nextads_stability_audit.md"
)
SOURCE_PATTERNS = {
    "rand": re.compile(r"\bF\.rand\s*\("),
    "sampleBy": re.compile(r"\.sampleBy\s*\("),
    "collect_list": re.compile(r"\b(?:F\.)?collect_list\s*\("),
    "collect_set": re.compile(r"\b(?:F\.)?collect_set\s*\("),
    "max_by": re.compile(r"\b(?:F\.)?max_by\s*\(", re.IGNORECASE),
    "deduplicate": re.compile(
        r"\.(?:dropDuplicates|drop_duplicates|distinct)\s*\("
    ),
    "delete_from_and_load": re.compile(
        r"\bdelete_from_and_load\s*\("
    ),
    "truncate_and_load": re.compile(r"\btruncate_and_load\s*\("),
    "OPTIMIZE": re.compile(r"\bOPTIMIZE\b", re.IGNORECASE),
    "VACUUM": re.compile(r"\bVACUUM\b", re.IGNORECASE),
    "saveAsTable": re.compile(r"\.saveAsTable\s*\("),
    "DELETE FROM": re.compile(
        r"\bDELETE\s+FROM\b",
        re.IGNORECASE,
    ),
    "TRUNCATE TABLE": re.compile(
        r"\bTRUNCATE\s+TABLE\b",
        re.IGNORECASE,
    ),
    "overwrite_mode": re.compile(
        r"""\.mode\(\s*["']overwrite["']\s*\)""",
        re.IGNORECASE,
    ),
}
SOURCE_SUFFIXES = {".py", ".sql"}
RESOURCE_REFERENCE = re.compile(
    r"^\s+(?:python_file|notebook_path|include):\s*(.+?)\s*$",
    re.MULTILINE,
)
WORKSPACE_PREFIX = "${workspace.file_path}/"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore")


def _source_files():
    for source_root in ("jobs", "src"):
        for path in (PROJECT_ROOT / source_root).rglob("*"):
            if path.is_file() and path.suffix in SOURCE_SUFFIXES:
                yield path


def _relative(path: Path) -> str:
    return path.relative_to(PROJECT_ROOT).as_posix()


def _audit_block(name: str) -> list[str]:
    source = _read(AUDIT_PATH)
    start = f"<!-- {name}:start -->"
    end = f"<!-- {name}:end -->"
    assert source.count(start) == 1
    assert source.count(end) == 1
    return source.split(start, 1)[1].split(end, 1)[0].splitlines()


def _table_rows(name: str, expected_cells: int) -> list[list[str]]:
    rows = []
    for line in _audit_block(name):
        if not line.startswith("| `"):
            continue
        cells = [
            cell.strip()
            for cell in line.strip().strip("|").split("|")
        ]
        assert len(cells) == expected_cells, line
        cells[0] = cells[0].strip("`")
        rows.append(cells)
    assert rows, f"No audit rows found in {name}"
    return rows


def _reviewed_source_findings():
    findings = {}
    for path, pattern, count, reachability, disposition, rationale in (
        _table_rows("source-findings", 6)
    ):
        pattern = pattern.strip("`")
        assert pattern in SOURCE_PATTERNS
        assert reachability
        assert disposition
        assert len(rationale) >= 30
        key = (path, pattern)
        assert key not in findings
        findings[key] = int(count)
    return findings


def _actual_source_findings():
    findings = {}
    for path in _source_files():
        source = _read(path)
        for pattern_name, pattern in SOURCE_PATTERNS.items():
            count = len(pattern.findall(source))
            if count:
                findings[(_relative(path), pattern_name)] = count
    return findings


def _normalise_source_context(line: str) -> str:
    return re.sub(r"\s+", " ", line.strip())


def _source_context_fingerprint(path: Path) -> str:
    source = _read(path)
    lines = source.splitlines()
    contexts = []
    for pattern_name, pattern in sorted(SOURCE_PATTERNS.items()):
        for match in pattern.finditer(source):
            line_index = source.count("\n", 0, match.start())
            context = "\n".join(
                _normalise_source_context(line)
                for line in lines[
                    max(0, line_index - 2) : min(
                        len(lines),
                        line_index + 3,
                    )
                ]
            )
            contexts.append(f"{pattern_name}\n{context}")

    assert contexts
    content = "\n---\n".join(contexts).encode("utf-8")
    return hashlib.sha256(content).hexdigest()[:16]


def _actual_source_context_fingerprints():
    return {
        _relative(path): _source_context_fingerprint(path)
        for path in _source_files()
        if any(
            pattern.search(_read(path))
            for pattern in SOURCE_PATTERNS.values()
        )
    }


def _reviewed_source_context_fingerprints():
    findings = {}
    for path, fingerprint in _table_rows(
        "source-context-fingerprints",
        2,
    ):
        fingerprint = fingerprint.strip("`")
        assert re.fullmatch(r"[0-9a-f]{16}", fingerprint)
        assert path not in findings
        findings[path] = fingerprint
    return findings


def _contains_window_constructor(node: ast.AST) -> bool:
    return any(
        isinstance(part, ast.Name) and part.id == "Window"
        for part in ast.walk(node)
    )


def _actual_python_window_findings():
    counts = Counter()
    for path in _source_files():
        if path.suffix != ".py":
            continue
        tree = ast.parse(_read(path))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "orderBy"
                and _contains_window_constructor(node.func.value)
            ):
                counts[_relative(path)] += 1
    return dict(counts)


def _reviewed_window_findings(name: str):
    findings = {}
    for path, count, reachability, disposition, rationale in _table_rows(
        name,
        5,
    ):
        assert reachability
        assert disposition
        assert len(rationale) >= 30
        assert path not in findings
        findings[path] = int(count)
    return findings


def _sql_string_literals(path: Path):
    if path.suffix == ".sql":
        return [_read(path)]

    tree = ast.parse(_read(path))
    parents = {
        child: node
        for node in ast.walk(tree)
        for child in ast.iter_child_nodes(node)
    }
    literals = []
    for node in ast.walk(tree):
        if isinstance(node, ast.JoinedStr):
            literals.append(
                "".join(
                    value.value
                    for value in node.values
                    if isinstance(value, ast.Constant)
                    and isinstance(value.value, str)
                )
            )
        elif (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and not isinstance(parents.get(node), ast.JoinedStr)
        ):
            literals.append(node.value)
    return literals


def _actual_sql_window_findings():
    pattern = re.compile(r"\bOVER\s*\(", re.IGNORECASE)
    findings = {}
    for path in _source_files():
        count = sum(
            len(pattern.findall(literal))
            for literal in _sql_string_literals(path)
        )
        if count:
            findings[_relative(path)] = count
    return findings


def _resolve_resource_reference(resource_path: Path, value: str):
    value = value.strip().strip("\"'")
    if value.startswith(WORKSPACE_PREFIX):
        candidate = PROJECT_ROOT / value.removeprefix(WORKSPACE_PREFIX)
    elif value.startswith("."):
        candidate = (resource_path.parent / value).resolve()
    else:
        return None

    candidates = [candidate]
    if not candidate.suffix:
        candidates.extend(
            candidate.with_suffix(suffix)
            for suffix in (".py", ".sql")
        )
    existing = [path for path in candidates if path.is_file()]
    assert len(existing) == 1, (
        f"Could not resolve one local entrypoint for {value!r} "
        f"in {_relative(resource_path)}"
    )
    resolved = existing[0].resolve()
    resolved.relative_to(PROJECT_ROOT.resolve())
    return _relative(resolved)


def _bundle_entrypoints():
    bundle = yaml.safe_load(_read(PROJECT_ROOT / "databricks.yml"))
    entrypoints = set()
    for resource in bundle["include"]:
        resource_path = PROJECT_ROOT / resource
        if not resource_path.is_file():
            continue
        for value in RESOURCE_REFERENCE.findall(_read(resource_path)):
            resolved = _resolve_resource_reference(resource_path, value)
            if resolved:
                entrypoints.add(resolved)
    return entrypoints


def _reviewed_bundle_exclusions():
    exclusions = {}
    for path, reachability, disposition, rationale in _table_rows(
        "bundle-exclusions",
        4,
    ):
        assert reachability == "Deployed analytics experiment"
        assert disposition == "Out of scope experiment"
        assert len(rationale) >= 30
        assert path not in exclusions
        exclusions[path] = rationale
    return exclusions


def test_every_remaining_stability_pattern_has_a_durable_review():
    assert _actual_source_findings() == _reviewed_source_findings()


def test_reviewed_source_findings_are_anchored_to_source_context():
    reviewed_paths = {
        path
        for path, _ in _reviewed_source_findings()
    }
    reviewed_fingerprints = _reviewed_source_context_fingerprints()

    assert set(reviewed_fingerprints) == reviewed_paths
    assert (
        _actual_source_context_fingerprints()
        == reviewed_fingerprints
    )


def test_every_direct_python_window_has_a_durable_review():
    assert _actual_python_window_findings() == _reviewed_window_findings(
        "window-findings"
    )


def test_every_sql_window_has_a_durable_review():
    assert _actual_sql_window_findings() == _reviewed_window_findings(
        "sql-window-findings"
    )


def test_bundle_entrypoints_are_scanned_or_explicitly_bounded():
    entrypoints = _bundle_entrypoints()
    scanned = {
        _relative(path)
        for path in _source_files()
    }
    exclusions = set(_reviewed_bundle_exclusions())

    assert {
        "jobs/nextads_data/archive_sort_order_data.py",
        "jobs/nextads_delivery/plp_gs.py",
        "jobs/realtime/viewed_bought.py",
        "src/next_ads/data/sort_order/data_pull.py",
        "src/next_ads/ranking/theme_affinity/dlt_pipeline.py",
    }.issubset(entrypoints)
    assert entrypoints - scanned == exclusions
    assert exclusions.issubset(entrypoints)


def test_unbundled_findings_are_not_direct_bundle_entrypoints():
    entrypoints = _bundle_entrypoints()
    for row in _table_rows("source-findings", 6):
        path, _, _, reachability, _, _ = row
        if reachability.startswith("Unbundled"):
            assert path not in entrypoints


def test_incident_route_has_no_retry_unsafe_writer_or_housekeeping():
    critical_files = {
        "jobs/nextads_data/archive_sort_order_data.py",
        "jobs/realtime/viewed_bought.py",
        "src/next_ads/delivery/google_sheets.py",
    }
    prohibited_patterns = {
        "rand",
        "sampleBy",
        "delete_from_and_load",
        "truncate_and_load",
        "OPTIMIZE",
        "VACUUM",
        "saveAsTable",
        "DELETE FROM",
        "TRUNCATE TABLE",
    }
    actual = _actual_source_findings()

    assert not {
        finding
        for finding in actual
        if finding[0] in critical_files
        and finding[1] in prohibited_patterns
    }

    for relative_path in critical_files:
        source = _read(PROJECT_ROOT / relative_path)
        assert ".dropDuplicates(" not in source
        assert ".drop_duplicates(" not in source


def test_active_membership_aggregates_are_canonical():
    expected_fragments = {
        "jobs/nextads_control/load_control_sheet_v2.py": (
            'F.sort_array(F.collect_set("PageType"))'
        ),
        "src/next_ads/control/load_control_sheet.py": (
            'F.sort_array(F.collect_set("Location"))'
        ),
        "src/next_ads/control/attributes.py": (
            'F.sort_array(F.collect_set("attribute_value"))'
        ),
        "src/next_ads/control/item_attributes.py": (
            'F.sort_array(F.collect_list("value"))'
        ),
        "src/next_ads/decisioning/assignment_publication.py": (
            'F.sort_array(F.collect_set("_masid_token"))'
        ),
        "src/next_ads/realtime/decisioning/"
        "advert_affinity_data_build.py": (
            'F.sort_array(F.collect_set("itemno"))'
        ),
    }

    for relative_path, fragment in expected_fragments.items():
        assert fragment in _read(PROJECT_ROOT / relative_path)


def test_audit_does_not_claim_owner_approval_or_runtime_proof():
    audit = _read(AUDIT_PATH)

    assert "It is not production runtime proof" in audit
    assert "not claim that an owner has approved" in audit
