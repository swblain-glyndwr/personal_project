# Pandera PySpark Data Validation Guide

Pandera is a data validation library that brings type hints and schema validation to PySpark DataFrames. It helps catch data quality issues early and ensures your data pipelines are robust.

**Official Documentation:** https://pandera.readthedocs.io/en/stable/pyspark.html

---

## **Quick Start**

### 1. Install Pandera

```bash
poetry add pandera
```

### 2. Define Your Schema

Create a schema file in your project:

```
next-ads/
├── next_ads/
│   ├── data_validation/
│   │   ├── schemas.py          # Data schemas
│   │   └── custom_checks.py    # Custom validation checks
└── ...
```

### 3. Create Schema (DataFrameModel)

**`next_ads/data_validation/schemas.py`**

### 4. Validate Your Data

```python
from next_ads.data_validation import schemas
from pyspark.sql import SparkSession
import json

spark = SparkSession.builder.appName("validation").getOrCreate()

# Read your data
df = spark.read.csv("data.csv", header=True)

# Validate
validated_df = schemas.ControlSheetInputModel.validate(df, lazy=True)
pandera_errors = validated_df.pandera.errors
print(json.dumps(dict(pandera_errors), indent=2))
print(f"Validation failed: {e}")
```

---

## **Custom Validation Checks**

Create reusable custom checks:

**`next_ads/data_validation/custom_checks.py`**

---

## **Common Validation Patterns**

### Check Value in List

```python
Realm: StringType = pa.Field(
    isin_spark={"allowed_values": ["Next"]}
)
```

### Check Regex Pattern

```python
url: StringType = pa.Field(
    str_matches_spark={"pattern": r"^/"}
)
```

### Check Nullable

```python
Status: StringType = pa.Field(
    nullable=True  # Allows NULL values
)
```

---

## **Usage in Scripts**

### Validate Input Data

```python
from next_ads.data_validation import schemas

@pa.check_input(schemas.ControlSheetInputModel, lazy=True)
def process_control_sheet(df):
    """Process control sheet with automatic validation"""
    # Your transformation logic
    return df
```

### Validate Output Data

```python
@pa.check_output(schemas.ControlSheetOutputModel, lazy=True)
def build_page(df):
    """Build page with output validation"""
    # Your transformation logic
    return df
```

---

## **Useful Resources**

- [Pandera Official Docs](https://pandera.readthedocs.io/)
- [PySpark API Reference](https://pandera.readthedocs.io/en/stable/pyspark.html)
- [Custom Checks Guide](https://pandera.readthedocs.io/en/stable/extensions.html)
