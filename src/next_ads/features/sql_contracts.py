"""Helpers for reading feature-store SQL contract files."""

from __future__ import annotations


def _extract_outer_column_block(create_table_sql: str) -> str:
    """Return the text inside the CREATE TABLE column-list parentheses."""
    start = create_table_sql.find("(")
    if start == -1:
        return ""

    depth = 0
    for index, char in enumerate(create_table_sql[start:], start=start):
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return create_table_sql[start + 1 : index]

    return ""


def _split_top_level_column_definitions(column_block: str) -> list[str]:
    """Split column definitions without splitting inside STRUCT/ARRAY types."""
    definitions = []
    current = []
    angle_depth = 0
    paren_depth = 0

    for char in column_block:
        if char == "<":
            angle_depth += 1
        elif char == ">" and angle_depth > 0:
            angle_depth -= 1
        elif char == "(":
            paren_depth += 1
        elif char == ")" and paren_depth > 0:
            paren_depth -= 1

        if char == "," and angle_depth == 0 and paren_depth == 0:
            definition = "".join(current).strip()
            if definition:
                definitions.append(definition)
            current = []
            continue

        current.append(char)

    definition = "".join(current).strip()
    if definition:
        definitions.append(definition)

    return definitions


def extract_create_table_columns(create_table_sql: str) -> list[tuple[str, str]]:
    """Extract top-level column definitions from a CREATE TABLE statement."""
    columns = []
    column_block = _extract_outer_column_block(create_table_sql)

    for definition in _split_top_level_column_definitions(column_block):
        line = " ".join(definition.split())
        upper_line = line.upper()
        if upper_line.startswith(("CONSTRAINT", "PRIMARY KEY")):
            continue

        parts = line.split(maxsplit=1)
        if len(parts) < 2:
            continue

        columns.append((parts[0].strip("`"), parts[1]))

    return columns
