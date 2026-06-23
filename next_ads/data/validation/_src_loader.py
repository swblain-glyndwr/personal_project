"""Load moved validation modules from the src package during transition."""

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import sys


def load_src_validation_module(module_name: str):
    src_module_name = f"_next_ads_src_data_validation_{module_name}"
    if src_module_name in sys.modules:
        return sys.modules[src_module_name]

    module_path = (
        Path(__file__).resolve().parents[3]
        / "src"
        / "next_ads"
        / "data"
        / "validation"
        / f"{module_name}.py"
    )
    spec = spec_from_file_location(src_module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load src validation module: {module_path}")

    module = module_from_spec(spec)
    sys.modules[src_module_name] = module
    spec.loader.exec_module(module)
    return module
