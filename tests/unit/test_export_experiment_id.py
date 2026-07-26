from next_ads import Export


class FakeColumn:
    def __init__(self, name):
        self.name = name

    def __eq__(self, value):
        """Record equality comparisons used to build Spark expressions."""
        return self.name, value


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


class FakeFunctions:
    @staticmethod
    def col(name):
        return FakeColumn(name)

    @staticmethod
    def lit(value):
        return value

    @staticmethod
    def when(condition, value):
        return FakeExpression(condition, value)

    @staticmethod
    def concat_ws(separator, *values):
        return separator, values


class FakeDataFrame:
    def __init__(self):
        self.expressions = {}

    def select(self, *columns):
        for column in columns:
            if isinstance(column, FakeExpression):
                self.expressions[column.name] = column
        return self

    def withColumn(self, _name, _value):  # noqa: N802 - mirrors PySpark API
        return self


def test_false_audience_split_is_exported_as_z(monkeypatch):
    monkeypatch.setattr(Export, "F", FakeFunctions())
    df = FakeDataFrame()

    Export.generate_experimentid(
        df,
        [{"Audience": {"AudienceTest": "Audience"}}],
        audience_sample=["Best"],
        audience_split=["VIP", "false"],
    )

    audience_expression = df.expressions["AudienceTest"]
    assert (("Audience", "VIP"), "Aud_AudienceTest_VIP") in (
        audience_expression.cases
    )
    assert (("Audience", "false"), "Aud_AudienceTest_Z") in (
        audience_expression.cases
    )
    assert "Aud_AudienceTest_false" not in {
        value for _, value in audience_expression.cases
    }
