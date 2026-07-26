import pytest

from next_ads import Export


class FakeCondition:
    def __init__(self, column_name, expected):
        self.column_name = column_name
        self.expected = expected

    def evaluate(self, row):
        return row.get(self.column_name) == self.expected


class FakeColumn:
    def __init__(self, name):
        self.name = name

    def __eq__(self, value):
        """Build an equality condition used by the fake Spark expressions."""
        return FakeCondition(self.name, value)

    def evaluate(self, row):
        return row.get(self.name)


class FakeLiteral:
    def __init__(self, value):
        self.value = value

    def evaluate(self, _row):
        return self.value


class FakeExpression:
    def __init__(self, condition, value):
        self.cases = [(condition, value)]
        self.default = None
        self.name = None

    def when(self, condition, value):
        self.cases.append((condition, value))
        return self

    def otherwise(self, value):
        self.default = value
        return self

    def alias(self, name):
        self.name = name
        return self

    def evaluate(self, row):
        for condition, value in self.cases:
            if condition.evaluate(row):
                return value.evaluate(row)
        if self.default is not None:
            return self.default.evaluate(row)
        return None


class FakeConcat:
    def __init__(self, separator, values):
        self.separator = separator
        self.values = values

    def evaluate(self, row):
        values = [value.evaluate(row) for value in self.values]
        return self.separator.join(str(value) for value in values if value is not None)


class FakeFunctions:
    @staticmethod
    def col(name):
        return FakeColumn(name)

    @staticmethod
    def lit(value):
        return FakeLiteral(value)

    @staticmethod
    def when(condition, value):
        return FakeExpression(condition, value)

    @staticmethod
    def concat_ws(separator, *values):
        return FakeConcat(separator, values)


class FakeDataFrame:
    def __init__(self, rows):
        self.rows = rows

    def select(self, *columns):
        selected_rows = []
        for row in self.rows:
            selected = {}
            for column in columns:
                if column == "*":
                    selected.update(row)
                elif isinstance(column, str):
                    selected[column] = row.get(column)
                else:
                    selected[column.name] = column.evaluate(row)
            selected_rows.append(selected)
        return FakeDataFrame(selected_rows)

    def withColumn(self, name, value):  # noqa: N802 - mirrors PySpark API
        return FakeDataFrame(
            [{**row, name: value.evaluate(row)} for row in self.rows]
        )


@pytest.fixture(autouse=True)
def fake_spark_functions(monkeypatch):
    monkeypatch.setattr(Export, "F", FakeFunctions())


CURRENT_EXPERIMENTS = [
    {"NextAds": "FallowControl"},
    {"PageIsolation": "PageTypeIsolation"},
    {"NextGenAds": "AdHocABTest1"},
]


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        (
            ("NoAds", "Best", "AllPages", "A"),
            "NextAds_CT | PageIsolation_AP | NextGenAds_A",
        ),
        (
            ("Ads", "Basic", "PLP_Only", "B"),
            "NextAds_BA | PageIsolation_PL | NextGenAds_B",
        ),
        (
            ("Ads", "Best", "SB_Only", "A"),
            "NextAds_BE | PageIsolation_SB | NextGenAds_A",
        ),
        (
            ("Ads", "Other", "Unknown", "Other"),
            "NextAds_Z | PageIsolation_Z | NextGenAds_Z",
        ),
    ],
)
def test_current_experiment_id_format(row, expected):
    fallow_control, shopping_bag, page_isolation, next_gen_ads = row
    df = FakeDataFrame(
        [
            {
                "AccountNumber": "account-1",
                "FallowControl": fallow_control,
                "ShoppingBagTest1": shopping_bag,
                "PageTypeIsolation": page_isolation,
                "AdHocABTest1": next_gen_ads,
            }
        ]
    )

    result = Export.generate_experimentid(df, CURRENT_EXPERIMENTS)

    assert result.rows == [
        {"AccountNumber": "account-1", "ExperimentID": expected}
    ]


@pytest.mark.parametrize(
    ("page_isolation", "expected_suffix"),
    [
        ("AllPages", "AP"),
        ("PLP_Only", "PL"),
        ("SB_Only", "SB"),
        ("HP_Only", "HP"),
        ("OC_Only", "OC"),
        ("Unknown", "Z"),
    ],
)
def test_page_isolation_mappings(page_isolation, expected_suffix):
    df = FakeDataFrame(
        [
            {
                "AccountNumber": "account-1",
                "PageTypeIsolation": page_isolation,
            }
        ]
    )

    result = Export.generate_experimentid(
        df,
        [{"PageIsolation": "PageTypeIsolation"}],
    )

    assert result.rows[0]["ExperimentID"] == f"PageIsolation_{expected_suffix}"


@pytest.mark.parametrize(
    ("split", "expected_suffix"),
    [("A", "A"), ("B", "B"), ("Other", "Z")],
)
def test_standard_experiment_mappings(split, expected_suffix):
    df = FakeDataFrame(
        [{"AccountNumber": "account-1", "AdHocABTest2": split}]
    )

    result = Export.generate_experimentid(
        df,
        [{"Rank": "AdHocABTest2"}],
    )

    assert result.rows[0]["ExperimentID"] == f"Rank_{expected_suffix}"


def test_additional_experiments_preserve_configured_order():
    df = FakeDataFrame(
        [
            {
                "AccountNumber": "account-1",
                "FallowControl": "Ads",
                "ShoppingBagTest1": "Best",
                "PageTypeIsolation": "AllPages",
                "AdHocABTest2": "A",
                "AdHocABTest1": "B",
            }
        ]
    )
    experiments = [
        {"NextAds": "FallowControl"},
        {"PageIsolation": "PageTypeIsolation"},
        {"Rank": "AdHocABTest2"},
        {"NextGenAds": "AdHocABTest1"},
    ]

    result = Export.generate_experimentid(df, experiments)

    assert result.rows[0]["ExperimentID"] == (
        "NextAds_BE | PageIsolation_AP | Rank_A | NextGenAds_B"
    )


@pytest.mark.parametrize(
    ("fallow_control", "shopping_bag", "audience", "expected_suffix"),
    [
        ("NoAds", "Best", "VIP", "Z"),
        ("Ads", "Basic", "VIP", "Z"),
        ("Ads", "Best", "VIP", "VIP"),
        ("Ads", "Best", "false", "Z"),
    ],
)
def test_audience_experiment_mappings(
    fallow_control,
    shopping_bag,
    audience,
    expected_suffix,
):
    df = FakeDataFrame(
        [
            {
                "AccountNumber": "account-1",
                "FallowControl": fallow_control,
                "ShoppingBagTest1": shopping_bag,
                "Audience": audience,
            }
        ]
    )

    result = Export.generate_experimentid(
        df,
        [{"Audience": {"AudienceTest": "Audience"}}],
        audience_sample=["Best"],
        audience_split=["VIP", "false"],
    )

    assert result.rows[0]["ExperimentID"] == f"Aud_AudienceTest_{expected_suffix}"
