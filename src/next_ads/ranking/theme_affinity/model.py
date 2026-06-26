import os

import joblib
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.preprocessing import LabelEncoder


try:
    import mlflow.pyfunc as mlflow_pyfunc
except ModuleNotFoundError:
    class _PythonModel:
        pass

    mlflow_pyfunc = type("mlflow_pyfunc", (), {"PythonModel": _PythonModel})


class XGBoostRankingModel(mlflow_pyfunc.PythonModel):
    """PyFunc wrapper for Theme Affinity XGBoost ranking models."""

    EXCLUDED_FEATURE_COLUMNS = {
        "account_number",
        "label",
        "theme",
        "theme_clean",
        "rundate",
        "reference_date",
        "rules_rank_source",
        "views_dates",
        "atbs_dates",
        "baskets_dates",
        "target_dates",
        "split",
        "prediction",
        "model_version",
    }

    def __init__(self, feature_cols=None):
        self.feature_cols = feature_cols
        self.encoders = {}
        self.categorical_cols = []
        self.metadata = {}
        self.booster = None

    @classmethod
    def prepare_xgb_data(cls, df, feature_cols=None, encoders=None):
        df = df.copy()
        if feature_cols is None:
            feature_cols = [
                column
                for column in df.columns
                if column not in cls.EXCLUDED_FEATURE_COLUMNS
            ]

        fitted_encoders = {} if encoders is None else encoders
        is_training = encoders is None
        for column in feature_cols:
            if df[column].dtype == "object" or df[column].dtype.name == "category":
                if is_training:
                    encoder = LabelEncoder()
                    df[column] = encoder.fit_transform(df[column].astype(str))
                    fitted_encoders[column] = encoder
                    continue

                encoder = fitted_encoders[column]
                valid = df[column].astype(str).isin(encoder.classes_)
                safe_values = np.where(
                    valid,
                    df[column].astype(str),
                    encoder.classes_[0],
                )
                df[column] = encoder.transform(safe_values)
                df.loc[~valid, column] = -1

        df_sorted = df.sort_values("account_number").reset_index(drop=True)
        X = df_sorted[feature_cols].values.astype(np.float32)
        y = df_sorted["label"].values.astype(np.float32)
        groups = df_sorted.groupby("account_number").size().values
        return X, y, groups, feature_cols, fitted_encoders

    @staticmethod
    def ranking_metrics(preds, dtrain):
        labels = dtrain.get_label()
        group_ptr = dtrain.get_uint_info("group_ptr")
        discounts = 1.0 / np.log2(np.arange(2, 7))
        mrr_scores = []
        ndcg5_scores = []

        for index in range(len(group_ptr) - 1):
            start, end = group_ptr[index], group_ptr[index + 1]
            group_true = labels[start:end]
            group_pred = preds[start:end]
            if np.sum(group_true) <= 0:
                continue

            ranked_labels = group_true[np.argsort(-group_pred)]
            positive_indexes = np.where(ranked_labels == 1)[0]
            mrr_scores.append(
                1.0 / (positive_indexes[0] + 1)
                if len(positive_indexes) > 0
                else 0.0
            )
            top5_labels = ranked_labels[:5]
            actual_dcg = np.sum(top5_labels * discounts[: len(top5_labels)])
            ideal_dcg = np.sum(discounts[: min(int(np.sum(group_true)), 5)])
            ndcg5_scores.append(actual_dcg / ideal_dcg if ideal_dcg > 0 else 0.0)

        avg_mrr = np.mean(mrr_scores) if mrr_scores else 0.0
        avg_ndcg5 = np.mean(ndcg5_scores) if ndcg5_scores else 0.0
        return [("mrr", float(avg_mrr)), ("ndcg_at_5", float(avg_ndcg5))]

    def fit(
        self,
        df_train: pd.DataFrame,
        params: dict,
        df_val: pd.DataFrame | None = None,
        num_boost_round: int = 100,
        early_stopping_rounds: int = 20,
    ):
        X_train, y_train, groups_train, feature_cols, encoders = (
            self.prepare_xgb_data(df_train, feature_cols=self.feature_cols)
        )
        self.feature_cols = feature_cols
        self.encoders = encoders
        self.categorical_cols = list(encoders)
        self.metadata = {
            "feature_cols": feature_cols,
            "categorical_cols": self.categorical_cols,
            "params": params,
        }

        dtrain = xgb.DMatrix(X_train, label=y_train, feature_names=feature_cols)
        dtrain.set_group(groups_train)
        evals = [(dtrain, "train")]
        if df_val is not None:
            X_val, y_val, groups_val, _, _ = self.prepare_xgb_data(
                df_val,
                feature_cols=self.feature_cols,
                encoders=encoders,
            )
            dval = xgb.DMatrix(X_val, label=y_val, feature_names=feature_cols)
            dval.set_group(groups_val)
            evals.append((dval, "eval"))

        self.booster = xgb.train(
            params,
            dtrain,
            num_boost_round=num_boost_round,
            evals=evals,
            custom_metric=self.ranking_metrics,
            early_stopping_rounds=(
                early_stopping_rounds if df_val is not None else None
            ),
            maximize=True,
        )
        return self

    def artifacts(self, artifact_dir: str) -> dict[str, str]:
        os.makedirs(artifact_dir, exist_ok=True)
        model_path = os.path.join(artifact_dir, "theme_affinity_ranker.pkl")
        joblib.dump(self, model_path)
        return {"model": model_path}

    def load_context(self, context):
        loaded = joblib.load(context.artifacts["model"])
        self.__dict__.update(loaded.__dict__)

    def predict(self, context, model_input):
        df = model_input.copy()
        for column in self.categorical_cols:
            if column not in df or column not in self.encoders:
                continue
            encoder = self.encoders[column]
            valid = df[column].astype(str).isin(encoder.classes_)
            safe_values = np.where(
                valid,
                df[column].astype(str),
                encoder.classes_[0],
            )
            df[column] = encoder.transform(safe_values)
            df.loc[~valid, column] = -1

        matrix = xgb.DMatrix(
            df[self.feature_cols].values.astype(np.float32),
            feature_names=self.feature_cols,
        )
        return self.booster.predict(matrix)

    def evaluate(self, df: pd.DataFrame) -> dict[str, float]:
        X, y_true, groups, _, _ = self.prepare_xgb_data(
            df,
            feature_cols=self.feature_cols,
            encoders=self.encoders,
        )
        dmatrix = xgb.DMatrix(X, label=y_true, feature_names=self.feature_cols)
        dmatrix.set_group(groups)
        y_pred = self.booster.predict(dmatrix)
        group_ptr = dmatrix.get_uint_info("group_ptr")
        labels = dmatrix.get_label()
        discounts5 = 1.0 / np.log2(np.arange(2, 7))
        discounts32 = 1.0 / np.log2(np.arange(2, 34))
        mrr_scores = []
        ndcg5_scores = []
        ndcg32_scores = []
        hit1_scores = []
        hit3_scores = []
        hit5_scores = []

        for index in range(len(group_ptr) - 1):
            start, end = group_ptr[index], group_ptr[index + 1]
            group_true = labels[start:end]
            group_pred = y_pred[start:end]
            if np.sum(group_true) <= 0:
                continue

            ranked_labels = group_true[np.argsort(-group_pred)]
            positive_indexes = np.where(ranked_labels == 1)[0]
            first_positive = (
                int(positive_indexes[0]) if len(positive_indexes) > 0 else None
            )
            mrr_scores.append(
                1.0 / (first_positive + 1)
                if first_positive is not None
                else 0.0
            )
            ndcg5_scores.append(_ndcg(ranked_labels, group_true, discounts5, 5))
            ndcg32_scores.append(_ndcg(ranked_labels, group_true, discounts32, 32))
            hit1_scores.append(_hit_at(first_positive, 1))
            hit3_scores.append(_hit_at(first_positive, 3))
            hit5_scores.append(_hit_at(first_positive, 5))

        return {
            "mrr": _mean(mrr_scores),
            "ndcg_at_5": _mean(ndcg5_scores),
            "ndcg_at_32": _mean(ndcg32_scores),
            "hit_at_1": _mean(hit1_scores),
            "hit_at_3": _mean(hit3_scores),
            "hit_at_5": _mean(hit5_scores),
        }


def _ndcg(ranked_labels, group_true, discounts, cutoff):
    top_labels = ranked_labels[:cutoff]
    actual_dcg = np.sum(top_labels * discounts[: len(top_labels)])
    ideal_dcg = np.sum(discounts[: min(int(np.sum(group_true)), cutoff)])
    return float(actual_dcg / ideal_dcg) if ideal_dcg > 0 else 0.0


def _hit_at(first_positive_index, cutoff):
    if first_positive_index is None:
        return 0.0
    return float(first_positive_index < cutoff)


def _mean(values):
    return float(np.mean(values)) if values else 0.0
