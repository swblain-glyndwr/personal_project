import ast
from datetime import date
import inspect
import json
from pathlib import Path

import pytest

from next_ads.features import pctr_affinity
from next_ads.features.analytics_pctr_source import (
    AnalyticsPctrSourceDefinition,
    load_analytics_pctr_source_definition,
    with_producing_run_id,
)
from next_ads.features.pctr_affinity import (
    ACCOUNT_ADVERT_AFFINITY_COLUMNS,
    ANALYTICS_PCTR_MODEL_INPUT_COLUMNS,
    CUSTOMER_ADVERT_IMPRESSIONS_30D_SOURCE_COLUMNS,
    RULES_BASED_PCTR_SOURCE_COLUMNS,
    SESSION_CONTEXT_COLUMNS,
    DeltaSourceBinding,
    bind_analytics_pctr_source,
    build_account_advert_affinity_frame,
    build_analytics_pctr_model_input_frame,
    build_session_context_frame,
    parse_optional_delta_version,
    serialise_source_binding,
)


REFERENCE_DATE = date(2026, 8, 1)
SOURCE_TABLE = (
    "marketingdata_dev.nextads_integration."
    "next_uk_nextAds_analytics_pctr_features"
)


@pytest.fixture(scope="module")
def local_spark():
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        pytest.skip(f"PySpark unavailable: {exc}")
    try:
        return (
            SparkSession.builder.master("local[2]")
            .appName("next-ads-pctr-affinity-tests")
            .getOrCreate()
        )
    except (RuntimeError, ValueError) as exc:
        pytest.skip(f"Local Spark unavailable: {exc}")


def test_module_keeps_pyspark_imports_inside_transform_functions():
    tree = ast.parse(inspect.getsource(pctr_affinity))
    top_level_imports = [
        node
        for node in tree.body
        if isinstance(node, (ast.Import, ast.ImportFrom))
    ]

    assert all(
        not (
            isinstance(node, ast.ImportFrom)
            and node.module
            and node.module.startswith("pyspark")
        )
        and not (
            isinstance(node, ast.Import)
            and any(alias.name.startswith("pyspark") for alias in node.names)
        )
        for node in top_level_imports
    )


@pytest.mark.parametrize(
    ("value", "expected"),
    [(None, None), ("", None), ("latest", None), ("12", 12), (0, 0)],
)
def test_optional_delta_version_is_explicit(value, expected):
    assert parse_optional_delta_version(value) == expected


@pytest.mark.parametrize("value", [-1, "-1", "moving", True])
def test_optional_delta_version_rejects_invalid_values(value):
    with pytest.raises(ValueError, match="non-negative integer or latest"):
        parse_optional_delta_version(value)


def test_exact_source_binding_is_json_serialisable():
    binding = DeltaSourceBinding(
        source_role="analytics_pctr_features",
        table_path=SOURCE_TABLE,
        delta_version=31,
        reference_date=REFERENCE_DATE,
    )

    assert json.loads(serialise_source_binding(binding)) == {
        "delta_version": 31,
        "producing_run_id": None,
        "reference_date": "2026-08-01",
        "reference_date_row_count": None,
        "schema_sha256": None,
        "source_role": "analytics_pctr_features",
        "table_id": None,
        "table_path": SOURCE_TABLE,
    }


def test_source_binding_records_the_exact_producing_job_run():
    binding = DeltaSourceBinding(
        source_role="analytics_pctr_features",
        table_path=SOURCE_TABLE,
        delta_version=31,
        reference_date=REFERENCE_DATE,
    )

    receipt = with_producing_run_id(binding, "123456789")

    assert receipt.producing_run_id == "123456789"
    assert binding.producing_run_id is None
    with pytest.raises(ValueError, match="producing_run_id"):
        with_producing_run_id(binding, "")


def test_source_bindings_keep_personal_and_shared_dev_separate(tmp_path):
    personal_path = tmp_path / "personal.yaml"
    personal_path.write_text(
        """analytics_pctr_source:
  scope: PERSONAL_DEV
  catalog: "{catalog}"
  schema: "{schema}"
  table_name: next_uk_nextads_analytics_pctr_features
  delta_version: latest
  fixed_reference_date: requested
"""
    )
    personal = load_analytics_pctr_source_definition(
        personal_path,
        catalog="marketingdata_dev",
        schema="stephen_blain",
    )
    assert personal.table_path == (
        "marketingdata_dev.stephen_blain."
        "next_uk_nextads_analytics_pctr_features"
    )
    assert personal.receipt_table_path == (
        "marketingdata_dev.stephen_blain."
        "next_uk_nextads_analytics_pctr_feature_source_receipts"
    )
    with pytest.raises(ValueError, match="Shared DEV Feature Store"):
        personal.validate_target(
            catalog="marketingdata_dev",
            schema="nextads_feature_store",
        )

    shared = load_analytics_pctr_source_definition(
        Path("configs/features/analytics_pctr_source_dev.yaml")
    )
    shared.validate_target(
        catalog="marketingdata_dev",
        schema="nextads_feature_store",
    )
    with pytest.raises(ValueError, match="Personal Feature Store"):
        shared.validate_target(
            catalog="marketingdata_dev",
            schema="stephen_blain",
        )


def test_source_reader_resolves_latest_once_and_reads_that_exact_version():
    class Result:
        def __init__(self, row):
            self.row = row

        def first(self):
            return self.row

    class Schema:
        @staticmethod
        def json():
            return json.dumps(
                {
                    "type": "struct",
                    "fields": [
                        {
                            "name": "rundate",
                            "type": "date",
                            "nullable": False,
                            "metadata": {},
                        }
                    ],
                }
            )

    class Frame:
        columns = ["rundate"]
        schema = Schema()

        def where(self, _condition):
            return self

        @staticmethod
        def count():
            return 10

    class Reader:
        def __init__(self):
            self.options = []
            self.tables = []

        def option(self, key, value):
            self.options.append((key, value))
            return self

        def table(self, table_path):
            self.tables.append(table_path)
            return Frame()

    class Spark:
        def __init__(self):
            self.queries = []
            self.read = Reader()

        def sql(self, query):
            self.queries.append(query)
            if query.startswith("DESCRIBE HISTORY"):
                return Result({"version": 31})
            return Result({"id": "table-id-1"})

    spark = Spark()

    definition = AnalyticsPctrSourceDefinition(
        scope="SHARED_DEV",
        catalog="marketingdata_dev",
        schema="nextads_integration",
        table_name="next_uk_nextAds_analytics_pctr_features",
        delta_version=None,
        fixed_reference_date=None,
    )
    binding, frame = bind_analytics_pctr_source(
        spark,
        definition=definition,
        reference_date=REFERENCE_DATE,
    )

    assert binding.delta_version == 31
    assert isinstance(frame, Frame)
    assert binding.table_id == "table-id-1"
    assert binding.reference_date_row_count == 10
    assert len(binding.schema_sha256) == 64
    assert spark.queries == [
        "DESCRIBE HISTORY `marketingdata_dev`.`nextads_integration`."
        "`next_uk_nextAds_analytics_pctr_features` LIMIT 1",
        "DESCRIBE DETAIL `marketingdata_dev`.`nextads_integration`."
        "`next_uk_nextAds_analytics_pctr_features`",
    ]
    assert spark.read.options == [("versionAsOf", 31)]
    assert spark.read.tables == [SOURCE_TABLE]


def _analytics_output(local_spark, *, duplicate=False, include_optional=False):
    rows = [
        (
            "A-1",
            "P1_C1_V1",
            None,
            REFERENCE_DATE,
            1.25,
            0.75,
            14,
            9,
            0.33,
            4 if include_optional else None,
            14 if include_optional else None,
            0.33 if include_optional else None,
        ),
        (
            "A-2",
            "P2_C2_V1",
            "shopping_bag_1",
            date(2026, 7, 31),
            2.0,
            1.0,
            3,
            2,
            0.2,
            None,
            None,
            None,
        ),
    ]
    if duplicate:
        rows.append(rows[0])
    return local_spark.createDataFrame(
        rows,
        "account_number string, UniqueAdID string, location string, "
        "rundate date, view_lift_adjusted double, "
        "purchase_lift_adjusted double, "
        "customer_advert_previous_impression_number long, "
        "number_impressions_same_algodivision long, advert_scoring double, "
        "customer_advert_impressions_7d long, "
        "customer_advert_impressions_30d long, rules_based_pctr double",
    )


def test_account_advert_adapter_maps_documented_analytics_fields(local_spark):
    result = build_account_advert_affinity_frame(
        _analytics_output(local_spark),
        REFERENCE_DATE,
    )
    row = result.collect()[0]

    assert tuple(result.columns) == ACCOUNT_ADVERT_AFFINITY_COLUMNS
    assert row.account_number == "A-1"
    assert row.advert_id == "P1_C1_V1"
    assert "location" not in result.columns
    assert row.reference_date == REFERENCE_DATE
    assert row.viewed_latest_advert_catid_affinity == pytest.approx(1.25)
    assert row.purchased_latest_advert_catid_affinity == pytest.approx(0.75)
    assert row.customer_advert_impressions_30d is None
    assert row.advert_algodivision_impressions == 9
    assert row.customer_advert_impressions_7d is None
    assert row.rules_based_pctr is None
    assert row.created_at == row.updated_at


def test_legacy_fields_are_not_exact_30_day_or_rules_based_sources():
    lookup = {
        "customer_advert_previous_impression_number": (
            "customer_advert_previous_impression_number"
        ),
        "advert_scoring": "advert_scoring",
    }

    assert (
        pctr_affinity._resolve_column(
            lookup,
            CUSTOMER_ADVERT_IMPRESSIONS_30D_SOURCE_COLUMNS,
            description="exact 30-day account-advert impressions",
            required=False,
        )
        is None
    )
    assert (
        pctr_affinity._resolve_column(
            lookup,
            RULES_BASED_PCTR_SOURCE_COLUMNS,
            description="rules-based pCTR",
            required=False,
        )
        is None
    )


def test_account_advert_adapter_uses_only_explicit_optional_fields(
    local_spark,
):
    result = build_account_advert_affinity_frame(
        _analytics_output(local_spark, include_optional=True),
        REFERENCE_DATE,
    )
    row = result.collect()[0]

    assert row.customer_advert_impressions_7d == 4
    assert row.customer_advert_impressions_30d == 14
    assert row.rules_based_pctr == pytest.approx(0.33)


def test_account_advert_adapter_rejects_duplicate_contract_keys(local_spark):
    with pytest.raises(ValueError, match="duplicate keys"):
        build_account_advert_affinity_frame(
            _analytics_output(local_spark, duplicate=True),
            REFERENCE_DATE,
        )


def test_account_advert_adapter_requires_the_approved_affinity_fields(
    local_spark,
):
    source = _analytics_output(local_spark).drop("view_lift_adjusted")

    with pytest.raises(
        ValueError, match="viewed_latest_advert_catid_affinity"
    ):
        build_account_advert_affinity_frame(source, REFERENCE_DATE)


def _analytics_model_input(local_spark):
    from pyspark.sql import functions as F

    source = _analytics_output(local_spark)
    typed_values = {
        "ad_clicked": (1, "int"),
        "treatment_type": ("Best", "string"),
        "age": (42, "int"),
        "cash_acc": (1, "int"),
        "advert_ctr": (0.11, "double"),
        "device_ctr": (0.12, "double"),
        "geo_ctr": (0.13, "double"),
        "gender_ctr": (0.14, "double"),
        "dod_ctr_change": (0.01, "double"),
        "wow_ctr_change": (0.02, "double"),
        "number_pages_viewed": (8, "int"),
        "prior_30_day_order_value": (75.5, "double"),
        "customer_total_clicks": (3, "int"),
        "customer_total_unique_adverts_clicked": (2, "int"),
        "customer_advert_previous_click_number": (1, "int"),
        "number_clicks_same_algodivision": (2, "int"),
        "advert_impressions": (120, "int"),
        "device_impressions": (80, "int"),
        "geo_impressions": (70, "int"),
        "gender_impressions": (60, "int"),
        "day_impressions": (15, "int"),
        "prior_day_impressions": (12, "int"),
        "view_theme_score": (0.2, "double"),
        "perc_order_value_cat_affinity": (0.3, "double"),
        "perc_30_day_order_value_cat_affinity": (0.4, "double"),
        "perc_order_qty_cat_affinity": (0.5, "double"),
        "view_highest_catid_weight": (0.6, "double"),
        "purchase_highest_catid_weight": (0.7, "double"),
        "purchase_theme_affinity": (0.8, "double"),
    }
    for column_name, (value, spark_type) in typed_values.items():
        source = source.withColumn(
            column_name,
            F.lit(value).cast(spark_type),
        )
    return source


def test_analytics_model_input_preserves_the_existing_model_features(
    local_spark,
):
    result = build_analytics_pctr_model_input_frame(
        _analytics_model_input(local_spark),
        REFERENCE_DATE,
    )
    row = result.collect()[0]

    assert tuple(result.columns) == ANALYTICS_PCTR_MODEL_INPUT_COLUMNS
    assert row.account_number == "A-1"
    assert row.advert_id == "P1_C1_V1"
    assert row.reference_date == REFERENCE_DATE
    assert row.ad_clicked == 1
    assert row.advert_ctr == pytest.approx(0.11)
    assert row.view_lift_adjusted == pytest.approx(1.25)
    assert row.purchase_lift_adjusted == pytest.approx(0.75)


def test_analytics_model_input_requires_every_existing_model_feature(
    local_spark,
):
    source = _analytics_model_input(local_spark).drop("advert_ctr")

    with pytest.raises(ValueError, match="advert_ctr"):
        build_analytics_pctr_model_input_frame(source, REFERENCE_DATE)


def _session_sources(local_spark, *, ambiguous=False):
    web_sessions = [
        (
            "visit-a",
            REFERENCE_DATE,
            "rpid-a",
            "Mobile",
            "Paid Search",
            "United Kingdom",
            10,
        ),
        (
            "visit-b",
            REFERENCE_DATE,
            "rpid-b",
            "Tablet",
            "Affiliate",
            "France",
            23,
        ),
    ]
    app_sessions = [
        (
            "visit-app",
            REFERENCE_DATE,
            "rpid-app",
            None,
            "Direct",
            "Ireland",
            8,
        )
    ]
    mappings = [
        ("rpid-a", "A-1"),
        ("rpid-b", "A-2"),
        ("rpid-app", "A-3"),
        ("rpid-ineligible", "A-4"),
    ]
    if ambiguous:
        web_sessions.append(
            (
                "visit-a",
                REFERENCE_DATE,
                "rpid-c",
                "Mobile",
                "Paid Search",
                "United Kingdom",
                10,
            )
        )
        mappings.append(("rpid-c", "A-3"))
    return (
        local_spark.createDataFrame(
            web_sessions,
            "UniqueVisitID string, date date, RPID string, Device string, "
            "Channel string, GeoCountry string, VisitStartHour int",
        ),
        local_spark.createDataFrame(
            app_sessions,
            "UniqueVisitID string, date date, RPID string, Device string, "
            "Channel string, GeoCountry string, VisitStartHour int",
        ),
        local_spark.createDataFrame(
            mappings,
            "roamingprofileid string, account_number string",
        ),
        local_spark.createDataFrame(
            [
                ("A-1", "GB", "NEXT"),
                ("A-2", "GB", "NEXT"),
                ("A-3", "GB", "NEXT"),
                ("A-4", "US", "NEXT"),
            ],
            "account_number string, countrycode string, client string",
        ),
        local_spark.createDataFrame(
            [
                ("visit-a", REFERENCE_DATE, "/shoppingbag/"),
                ("visit-a", REFERENCE_DATE, "/product/123"),
            ],
            "UniqueVisitID string, date date, PagePath string",
        ),
        local_spark.createDataFrame(
            [("France", "Europe")],
            "country_name string, segment_name string",
        ),
    )


def test_session_context_uses_unique_visit_id_and_bq_page_rules(local_spark):
    result = build_session_context_frame(
        *_session_sources(local_spark),
        REFERENCE_DATE,
    )
    rows = {row.session_id: row for row in result.collect()}

    assert tuple(result.columns) == SESSION_CONTEXT_COLUMNS
    assert set(rows) == {"visit-a", "visit-b", "visit-app"}
    assert rows["visit-a"].account_number == "A-1"
    assert rows["visit-a"].device_simple == "Mobile"
    assert rows["visit-a"].channel_simple == "Paid Search"
    assert rows["visit-a"].geocountry_simple == "UK & Ireland"
    assert rows["visit-a"].session_hour == 10
    assert rows["visit-a"].session_dayofweek == 7
    assert rows["visit-a"].session_is_weekend == 1
    assert rows["visit-a"].pages_in_session == 2
    assert rows["visit-a"].shopping_bag_pages_in_session == 1
    assert rows["visit-b"].device_simple == "Other"
    assert rows["visit-b"].channel_simple == "Other"
    assert rows["visit-b"].geocountry_simple == "Europe"
    assert rows["visit-b"].pages_in_session == 0
    assert rows["visit-b"].shopping_bag_pages_in_session == 0
    assert rows["visit-app"].account_number == "A-3"
    assert rows["visit-app"].device_simple == "App"
    assert rows["visit-app"].pages_in_session == 0


def test_session_context_rejects_unique_visit_id_with_two_accounts(
    local_spark,
):
    with pytest.raises(ValueError, match="maps to more than one account"):
        build_session_context_frame(
            *_session_sources(local_spark, ambiguous=True),
            REFERENCE_DATE,
        )


def test_session_context_chooses_one_deterministic_context_for_one_account(
    local_spark,
):
    sources = list(_session_sources(local_spark))
    sources[0] = sources[0].unionByName(
        local_spark.createDataFrame(
            [
                (
                    "visit-a",
                    REFERENCE_DATE,
                    "rpid-a",
                    "Desktop",
                    "Other",
                    "France",
                    11,
                )
            ],
            "UniqueVisitID string, date date, RPID string, Device string, "
            "Channel string, GeoCountry string, VisitStartHour int",
        )
    )

    first = build_session_context_frame(*sources, REFERENCE_DATE).where(
        "session_id = 'visit-a'"
    ).first()
    second = build_session_context_frame(*sources, REFERENCE_DATE).where(
        "session_id = 'visit-a'"
    ).first()

    assert first == second
    assert first.device_simple == "Mobile"
    assert first.channel_simple == "Paid Search"
    assert first.session_hour == 10
