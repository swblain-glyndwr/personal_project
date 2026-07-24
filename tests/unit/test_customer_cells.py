from next_ads.decisioning.customer_cells import ensure_audience_column


class FakeColumn:
    def cast(self, data_type):
        return f"cast:{data_type}"


class FakeFunctions:
    def __init__(self):
        self.calls = []

    def lit(self, value):
        self.calls.append(("lit", value))
        return f"lit:{value}"

    def col(self, name):
        self.calls.append(("col", name))
        return FakeColumn()

    def coalesce(self, *values):
        self.calls.append(("coalesce", values))
        return "coalesced"


class FakeDataFrame:
    def __init__(self, columns):
        self.columns = columns
        self.with_column_calls = []

    def withColumn(self, name, value):  # noqa: N802 - mirrors PySpark API
        self.with_column_calls.append((name, value))
        return self


def test_ensure_audience_column_adds_false_when_no_audience_config(monkeypatch):
    fake_functions = FakeFunctions()
    monkeypatch.setattr(
        "next_ads.decisioning.customer_cells.F",
        fake_functions,
    )
    df = FakeDataFrame(["AccountNumber", "ShoppingBagTest1"])

    result = ensure_audience_column(df)

    assert result is df
    assert df.with_column_calls == [("Audience", "lit:false")]


def test_ensure_audience_column_keeps_matches_and_defaults_unmatched_customers(
    monkeypatch,
):
    fake_functions = FakeFunctions()
    monkeypatch.setattr(
        "next_ads.decisioning.customer_cells.F",
        fake_functions,
    )
    df = FakeDataFrame(["AccountNumber", "Audience"])

    result = ensure_audience_column(df)

    assert result is df
    assert df.with_column_calls == [("Audience", "coalesced")]
    assert ("col", "Audience") in fake_functions.calls
    assert ("lit", "false") in fake_functions.calls
