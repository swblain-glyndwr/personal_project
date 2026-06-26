# Dynaconf Configuration Management

Dynaconf is a powerful configuration management tool for Python applications. It supports multiple config file formats (YAML, TOML, JSON, INI) and environment-based overrides.

**Official Documentation:** https://www.dynaconf.org/

---

## **Quick Start**

### 1. Install Dynaconf

```bash
poetry add dynaconf
```

### 2. Create Configuration Files

Create configuration files under `configs/runtime` in your project root:

```
next-ads/
  configs/
    runtime/
      settings.yaml          # Base configuration
      settings.local.yaml    # Local overrides (git-ignored)
      .env.local             # Local environment variable overrides (git-ignored)
```

### 3. Define Settings (YAML Format)

**`configs/runtime/settings.yaml`**

Default takes precedence, if ENV is set when loading config then environment specific values overwrite defaults.
```yaml
default:
  warehouse: marketingdata_dev
  schema: ds_sandbox
  NEW_VAR: 123

dev:
  warehouse: marketingdata_dev
  schema: ds_sandbox

prod:
  warehouse: marketingdata_prod
  schema: warehouse
```

**`configs/runtime/.env.local`**

Local environment variable overrides for configurations.
```env
USER_SCHEMA=first_lastname
```

### 4. Load Configuration in Your Code

**`next_ads/config/config_manager.py`**
```python
# Usage
from next_ads.utils import config_manager
JOB_ENV = "dev"
config = config_manager.load_config(JOB_ENV)
WAREHOUSE = config.catalog_read
```

---

## **Environment Variables Override**

Any setting can be overridden via environment variables with prefix `NEXT_ADS_`:

```bash
# Override via environment variable
export NEXT_ADS_DATABASE__HOST=staging-db.example.com
export NEXT_ADS_SPARK__MASTER=spark://staging-cluster:7077

python your_script.py
```
---

## **Useful Resources**

- 📚 [Dynaconf Official Docs](https://www.dynaconf.com/)
