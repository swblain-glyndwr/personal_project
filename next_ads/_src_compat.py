"""Import-path helpers for the temporary top-level package transition."""

from pathlib import Path


def extend_src_package_path(package_path, package_name: str) -> None:
    """Allow a top-level ``next_ads`` package to find matching ``src`` modules."""
    project_root = Path(__file__).resolve().parents[1]
    src_package = project_root / "src" / Path(*package_name.split("."))

    if src_package.is_dir():
        src_package_path = str(src_package)
        if src_package_path not in package_path:
            package_path.append(src_package_path)
