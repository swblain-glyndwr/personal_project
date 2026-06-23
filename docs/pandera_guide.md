# Pandera PySpark Data Validation Guide

Pandera is a data validation library that brings type hints and schema
validation to PySpark DataFrames. It helps catch data quality issues early and
keeps pipeline inputs and outputs explicit.

**Official Documentation:** https://pandera.readthedocs.io/en/stable/pyspark.html

---

## Quick Start

### 1. Install Pandera

```bash
poetry add pandera
```

### 2. Define Your Schema

Data validation code now lives in the reusable production package area:

```text
next-ads/
  src/
    next_ads/
      data/
        validation/
          schemas.py        # Data schemas
          custom_checks.py  # Custom validation checks
```

During the package migration, existing imports from
`next_ads.data_validation` remain supported by compatibility wrappers.

### 3. Create Schema (DataFrameModel)

**`src/next_ads/data/validation/schemas.py`**

### 4. Validate Your Data

```python
import json

from next_ads.data.validation import schemas
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("validation").getOrCreate()

# Read your data
df = spark.read.csv("data.csv", header=True)

# Validate
validated_df = schemas.ControlSheetInputModel.validate(df, lazy=True)
pandera_errors = validated_df.pandera.errors
print(json.dumps(dict(pandera_errors), indent=2))
```

---

## Custom Validation Checks

Create reusable custom checks in:

**`src/next_ads/data/validation/custom_checks.py`**

---

## Common Validation Patterns

### Check Value in List

```python
Realm: StringType = pa.Field(
    isin_spark={"allowed_values": ["Next"]},
)
```

### Check Regex Pattern

```python
url: StringType = pa.Field(
    str_matches_spark={"pattern": r"^/"},
)
```

### Check Nullable

```python
Status: StringType = pa.Field(
    nullable=True,  # Allows NULL values
)
```

---

## Usage In Scripts

### Validate Input Data

```python
from next_ads.data.validation import schemas


@pa.check_input(schemas.ControlSheetInputModel, lazy=True)
def process_control_sheet(df):
    """Process control sheet with automatic validation."""
    return df
```

### Validate Output Data

```python
from next_ads.data.validation import schemas


@pa.check_output(schemas.GlobalSolutionOutputModel, lazy=True)
def build_page(df):
    """Build page with output validation."""
    return df
```

---

## Useful Resources

- [Pandera Official Docs](https://pandera.readthedocs.io/)
- [PySpark API Reference](https://pandera.readthedocs.io/en/stable/pyspark.html)
- [Custom Checks Guide](https://pandera.readthedocs.io/en/stable/extensions.html)
