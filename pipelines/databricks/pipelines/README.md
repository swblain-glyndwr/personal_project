# Databricks Pipeline Resources

This folder defines Lakeflow/DLT pipeline resources used by Databricks jobs.

Any job that references a pipeline with `${resources.pipelines.<key>.id}` must
be available only in targets where that pipeline key is also declared. Keep job
and pipeline target blocks aligned so bundle planning does not fail with an
undeclared pipeline resource.

Python pipelines that import the canonical `next_ads` package must set
`root_path: ${workspace.file_path}/src`. Lakeflow adds this root to the Python
import path for pipeline execution. Do not replace it with source-level
`sys.path` bootstrapping or point it at the bundle root, where `next_ads` is not
directly importable.
