import ast
import copy
import inspect
import os
from pathlib import Path
import subprocess
import sys

from pyspark.serializers import CloudPickleSerializer


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PULL_SOURCE = (
    PROJECT_ROOT / "src" / "next_ads" / "data" / "sort_order" / "data_pull.py"
)
WORKER_FUNCTION_NAMES = {
    "call_next_cms_api_fn",
    "call_next_cms_api_duf",
    "call_next_api_fn",
    "call_next_api",
    "call_br_api_fn",
    "call_br_api",
    "parse_url_struct",
}
WORKER_ENTRYPOINT_NAMES = {
    "call_next_cms_api_duf",
    "call_next_api",
    "call_br_api",
    "parse_url_struct",
}


def _load_worker_functions():
    tree = ast.parse(DATA_PULL_SOURCE.read_text(), filename=str(DATA_PULL_SOURCE))
    selected_nodes = []

    for node in tree.body:
        if isinstance(node, ast.Import):
            imported_names = {alias.name for alias in node.names}
            if imported_names & {"json", "requests"}:
                selected_nodes.append(copy.deepcopy(node))
        elif isinstance(node, ast.ImportFrom) and node.module == "urllib.parse":
            selected_nodes.append(copy.deepcopy(node))
        elif (
            isinstance(node, ast.FunctionDef)
            and node.name in WORKER_FUNCTION_NAMES
        ):
            function_node = copy.deepcopy(node)
            function_node.decorator_list = []
            selected_nodes.append(function_node)

    worker_module = ast.Module(body=selected_nodes, type_ignores=[])
    ast.fix_missing_locations(worker_module)
    namespace = {
        "__builtins__": __builtins__,
        "__name__": "_lakeflow_sort_order_worker_payload",
    }
    exec(compile(worker_module, DATA_PULL_SOURCE, "exec"), namespace)
    return {
        name: namespace[name]
        for name in WORKER_ENTRYPOINT_NAMES
    }


def _transitive_function_globals(function):
    names = set()
    pending = [function]
    visited = set()

    while pending:
        current = pending.pop()
        if current in visited:
            continue
        visited.add(current)

        for name in current.__code__.co_names:
            names.add(name)
            value = current.__globals__.get(name)
            if inspect.isfunction(value):
                pending.append(value)

    return names


def test_sort_order_worker_functions_do_not_capture_nextads_config():
    worker_functions = _load_worker_functions()

    for name, function in worker_functions.items():
        assert "config" not in _transitive_function_globals(function), name


def test_sort_order_worker_payload_unpickles_without_repository_on_pythonpath(
    tmp_path,
):
    worker_functions = _load_worker_functions()
    payload_path = tmp_path / "sort-order-worker-payload.pkl"
    payload_path.write_bytes(
        CloudPickleSerializer().dumps(worker_functions)
    )

    environment = os.environ.copy()
    environment.pop("PYTHONPATH", None)
    completed = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            (
                "from pathlib import Path; "
                "from pyspark.serializers import CloudPickleSerializer; "
                "import sys; "
                "payload = Path(sys.argv[1]).read_bytes(); "
                "functions = CloudPickleSerializer().loads(payload); "
                "assert set(functions) == "
                "{'call_next_api', 'call_next_cms_api_duf', "
                "'call_br_api', 'parse_url_struct'}"
            ),
            str(payload_path),
        ],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr


def test_next_search_wrapper_is_passed_to_the_worker_as_a_scalar():
    source = DATA_PULL_SOURCE.read_text()

    assert "def call_next_api_fn(api_endpoint, url, next_search_wrapper):" in source
    assert "def call_next_api(api_endpoint, url, next_search_wrapper):" in source
    assert "F.lit(str(config.next_search_wrapper))" in source
