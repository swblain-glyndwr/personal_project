from pathlib import Path


def _install_numpy_pickle_compat():
    try:
        import numpy._core.multiarray  # noqa: F401
        return
    except ModuleNotFoundError:
        pass

    import importlib
    import sys

    sys.modules.setdefault("numpy._core", importlib.import_module("numpy.core"))
    sys.modules.setdefault(
        "numpy._core.multiarray",
        importlib.import_module("numpy.core.multiarray"),
    )


def _is_unity_catalog_model_uri(model_uri: str) -> bool:
    if not model_uri.startswith("models:/"):
        return False

    model_name = model_uri.removeprefix("models:/").strip("/").split("/", 1)[0]
    model_name = model_name.split("@", 1)[0]
    return model_name.count(".") >= 2


def _configure_mlflow_for_model_uri(mlflow, model_uri: str) -> None:
    mlflow.set_tracking_uri("databricks")
    if _is_unity_catalog_model_uri(model_uri):
        mlflow.set_registry_uri("databricks-uc")


def _load_mlflow_model(mlflow, model_uri: str, allow_spark: bool = False):
    if allow_spark:
        try:
            return "spark", mlflow.spark.load_model(model_uri)
        except Exception:
            pass

    try:
        return "xgboost", mlflow.xgboost.load_model(model_uri)
    except Exception as xgboost_error:
        try:
            return "pyfunc", mlflow.pyfunc.load_model(model_uri)
        except Exception:
            raise xgboost_error


def _predict_with_model(model_kind, model, raw_feature_pdf, encoders, model_input_cols):
    if model_kind == "pyfunc":
        return model.predict(raw_feature_pdf[model_input_cols])

    import numpy as np
    import xgboost as xgb

    feature_pdf = raw_feature_pdf[model_input_cols].copy()
    for column, encoder in encoders.items():
        if column not in feature_pdf:
            continue
        valid = feature_pdf[column].astype(str).isin(encoder.classes_)
        safe_values = np.where(
            valid,
            feature_pdf[column].astype(str),
            encoder.classes_[0],
        )
        feature_pdf[column] = encoder.transform(safe_values)
        feature_pdf.loc[~valid, column] = -1

    dmatrix = xgb.DMatrix(feature_pdf[model_input_cols])
    return model.predict(dmatrix)


def run_prediction(spark, runtime):
    import mlflow
    from pyspark.sql import functions as F

    _configure_mlflow_for_model_uri(mlflow, runtime.model_uri)
    model_config = runtime.config.ranking_model
    model_tables = runtime.config.ranking_model_tables
    model_input_cols = list(model_config.model_input_cols)
    output_cols = list(model_config.predict_table_cols)
    prediction_input_cols = _prediction_input_columns(model_input_cols, output_cols)
    rank_threshold = int(model_config.predict_rank_filter_threshold)

    predict_input = (
        spark.table(model_tables.predict_input_table)
        .filter(F.col("simple_rules_rank") <= rank_threshold)
        .select(*prediction_input_cols)
        .withColumnRenamed("theme_clean", "theme")
        .repartition(
            int(spark.conf.get("spark.default.parallelism", "200")),
            "account_number",
        )
    )

    prediction_schema = (
        predict_input.select(
            F.col("account_number"),
            F.col("theme"),
            F.col("month"),
            F.col("baskets_behavior__recency_rank"),
            F.lit(0.0).cast("float").alias("prediction"),
        ).schema
    )
    model_kind, model = _load_mlflow_model(
        mlflow,
        runtime.model_uri,
        allow_spark=True,
    )
    if model_kind == "spark":
        predictions = model.transform(predict_input).select(
            F.col("account_number"),
            F.col("theme"),
            F.col("month"),
            F.col("baskets_behavior__recency_rank"),
            F.col("prediction").cast("float").alias("prediction"),
        )
        (
            predictions.select(*output_cols)
            .write.mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(model_tables.predict_output_table)
        )
        return

    encoder_path = (
        runtime.project_root
        / "src"
        / "next_ads"
        / "ranking"
        / "theme_affinity"
        / "assets"
        / "ranking_encoders.joblib"
    )
    predictions = predict_input.mapInPandas(
        _predict_partition(runtime.model_uri, encoder_path, model_input_cols),
        schema=prediction_schema,
    )
    (
        predictions.select(*output_cols)
        .write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(model_tables.predict_output_table)
    )


def _prediction_input_columns(model_input_cols, output_cols):
    columns = list(dict.fromkeys(model_input_cols))
    for column in output_cols:
        if column in ("prediction", "theme"):
            continue
        if column not in columns:
            columns.append(column)
    if "account_number" not in columns:
        columns.append("account_number")
    if "theme_clean" not in columns:
        columns.append("theme_clean")
    return columns


def _predict_partition(model_uri: str, encoder_path: Path, model_input_cols):
    def predict_partition(iterator):
        import joblib
        import mlflow

        _configure_mlflow_for_model_uri(mlflow, model_uri)

        global _theme_affinity_predict_model
        try:
            model_kind, model = _theme_affinity_predict_model
        except NameError:
            _theme_affinity_predict_model = _load_mlflow_model(
                mlflow,
                model_uri,
            )
            model_kind, model = _theme_affinity_predict_model

        encoders = {}
        if model_kind == "xgboost":
            _install_numpy_pickle_compat()
            encoders = joblib.load(str(encoder_path))

        for pdf in iterator:
            if pdf.empty:
                yield pdf[
                    [
                        "account_number",
                        "theme",
                        "month",
                        "baskets_behavior__recency_rank",
                    ]
                ].assign(prediction=[])
                continue

            preds = _predict_with_model(
                model_kind,
                model,
                pdf[model_input_cols],
                encoders,
                model_input_cols,
            )
            result = pdf[
                [
                    "account_number",
                    "theme",
                    "month",
                    "baskets_behavior__recency_rank",
                ]
            ].copy()
            result["prediction"] = preds
            yield result

    return predict_partition
