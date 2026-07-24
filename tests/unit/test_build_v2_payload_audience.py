from jobs.nextads_delivery import build_v2_payload


class FakeDataFrame:
    def __init__(self, columns, rows=None):
        self.columns = columns
        self.rows = rows or []
        self.selected = None
        self.with_column_calls = []

    def select(self, *columns):
        df = FakeDataFrame(list(columns), self.rows)
        df.selected = columns
        return df

    def withColumn(self, name, value):  # noqa: N802 - mirrors PySpark API
        self.with_column_calls.append((name, value))
        if name not in self.columns:
            self.columns.append(name)
        return self

    def where(self, _condition):
        return self

    def distinct(self):
        seen = set()
        rows = []
        split_col = self.columns[0]
        for row in self.rows:
            value = row[split_col]
            if value in seen:
                continue
            seen.add(value)
            rows.append({split_col: value})
        return FakeDataFrame(self.columns, rows)

    def collect(self):
        return self.rows


class FakeColumn:
    def isNotNull(self):  # noqa: N802 - mirrors PySpark API
        return "is_not_null"

    def cast(self, data_type):
        return f"cast:{data_type}"


class FakeFunctions:
    def lit(self, value):
        return f"lit:{value}"

    def col(self, _name):
        return FakeColumn()

    def coalesce(self, *_values):
        return "coalesced"


class FakeLogger:
    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass


def test_normalize_audience_split_values_includes_false_once():
    assert build_v2_payload.normalize_audience_split_values(["VIP", "false", "VIP"]) == [
        "VIP",
        "false",
    ]
    assert build_v2_payload.normalize_audience_split_values(["VIP"]) == [
        "VIP",
        "false",
    ]


def test_assign_experiments_passes_false_audience_split(monkeypatch):
    fixed_cells = FakeDataFrame(
        ["AccountNumber", "FallowControl", "ShoppingBagTest1", "AdHocABTest1"]
    )
    customer_cells = FakeDataFrame(
        ["AccountNumber", "Audience"],
        [{"Audience": "VIP"}, {"Audience": "false"}],
    )
    captured = {}

    def fake_generate_experimentid(
        df,
        experiments,
        *,
        audience_df=None,
        audience_sample=None,
        audience_split=None,
    ):
        captured["audience_df"] = audience_df
        captured["audience_sample"] = audience_sample
        captured["audience_split"] = audience_split
        return df.select("AccountNumber")

    monkeypatch.setattr(
        build_v2_payload,
        "generate_experimentid",
        fake_generate_experimentid,
    )
    monkeypatch.setattr(build_v2_payload, "F", FakeFunctions())

    build_v2_payload.assign_experiments(
        customer_cells_fixed_latest=fixed_cells,
        customer_cells_latest=customer_cells,
        payload_experiment_settings={
            "experiments": {"PH7": "AdHocABTest1"},
            "audience_experiment": {
                "enabled": True,
                "name": "fathers_day_audience",
                "split_col": "Audience",
                "sample": ["Best"],
            },
        },
        logger=FakeLogger(),
    )

    assert set(captured["audience_split"]) == {"VIP", "false"}
    assert captured["audience_sample"] == ["Best"]
    assert captured["audience_df"].columns == ["AccountNumber", "Audience"]
    assert captured["audience_df"].with_column_calls == [("Audience", "coalesced")]
