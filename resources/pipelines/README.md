# Databricks Pipeline Resources

This folder defines Lakeflow/DLT pipeline resources used by Databricks jobs.

Any job that references a pipeline with `${resources.pipelines.<key>.id}` must
be available only in targets where that pipeline key is also declared. Keep job
and pipeline target blocks aligned so bundle planning does not fail with an
undeclared pipeline resource.
