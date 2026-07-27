# NextAds Dynaconf Configuration

Dynaconf is the source of truth for NextAds runtime and client configuration. Shared environment settings live under `configs/runtime/`, route settings live under folders such as `configs/control/` and `configs/adsv2/`, and client settings live under `configs/clients/`.

The migration away from `configs/clients/*.json` removes a duplicated configuration source. Before this change, jobs loaded table paths, Google Sheet sources, locations, and theme mapping values from JSON while other runtime settings already came from Dynaconf YAML. That made it possible for JSON and YAML to drift. With Dynaconf, the same environment-aware loader resolves dev, preprod, and prod settings before a Databricks job starts.

## Loading Config

Use `load_config()` for new code:

```python
from next_ads.common.config_manager import load_config

config = load_config(job_env="prod", client="next_uk")
theme_mapping_url = config.theme_mapping.url
control_sheet_v2_url = config.control_sheet_v2.url
assignments_table = config.tables_write.assignments_latest
```

`JOB_ENV` remains the Databricks/DAB environment selector. In Databricks, `JOB_ENV` must be supplied explicitly. `DATABRICKS_RUNTIME_VERSION` is used only to detect that a missing environment selector should fail loudly instead of falling back silently.

Client-aware jobs must pass the selected client into the loader, for example `load_config(JOB_ENV, client=CLIENT)`. This keeps `next_uk` and `next_gb` table paths, sheet sources, locations, and route settings separate at runtime.

## Compatibility Wrapper

`load_client_config(client)` still exists for older jobs, but it is now backed by Dynaconf and emits a `DeprecationWarning`. New code should not call it. Existing callers should move to `load_config(job_env, client)` as they are touched.

The compatibility wrapper keeps the legacy dict shape available during the transition, including the older `tables.read` and `tables.write` keys. This is intentional so the config source can move first without changing job outputs.

## Validation

`config_manager.py` registers Dynaconf validators during load. Required config such as `tables_read`, `tables_write`, `gcp`, `control_sheet`, `control_sheet_v2`, `exclusions_sheet`, `theme_mapping`, `theme_mapping_v2`, `locations`, catalogs, schemas, and client id must exist before the job can continue. Table path leaves must be non-empty strings.

This turns missing or malformed configuration into an immediate startup failure instead of allowing a Spark job to initialise and then fail later with a less useful error.

## Client Config

Client-specific settings are loaded from:

```text
configs/clients/next_uk.yaml
configs/clients/next_gb.yaml
```

The JSON client files are no longer an operational configuration source. If legacy JSON values are needed for migration evidence, keep them as fixtures rather than job-loaded config.
