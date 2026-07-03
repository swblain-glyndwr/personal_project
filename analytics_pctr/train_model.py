{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "7ad0205d-8eee-4280-8cb0-bdf5448b4a27",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# Two Stage Model\n",
    "\n",
    "* Primary Classification model is a high level 'popularity' model based on advert and customer features\n",
    "* Secondary Regression Model is a customer-advert 'affinity' model minimising the residuals from the first model using only affinity features \n",
    "\n",
    "\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1782125835783,
     "inputWidgets": {},
     "nuid": "ea78e5de-eb40-4954-a3c2-7ab72c40d75b",
     "showTitle": true,
     "startTime": 1782125833151,
     "submitTime": 1782125833099,
     "tableResultSettingsMap": {},
     "title": "Imports"
    }
   },
   "outputs": [],
   "source": [
    "import json\n",
    "import pandas as pd\n",
    "import mlflow\n",
    "import mlflow.spark\n",
    "from mlflow.models import infer_signature\n",
    "\n",
    "from datetime import datetime\n",
    "from pyspark.sql import functions as F\n",
    "from pyspark.sql import Window\n",
    "import matplotlib.pyplot as plt\n",
    "import seaborn as sns\n",
    "\n",
    "# Pipelining\n",
    "from pyspark.ml.feature import VectorAssembler, Imputer\n",
    "from pyspark.ml.pipeline import Pipeline\n",
    "from pyspark.ml.functions import vector_to_array\n",
    "\n",
    "# Models\n",
    "from xgboost.spark import SparkXGBClassifier, SparkXGBRegressor\n",
    "\n",
    "# ## Evaluation\n",
    "from pyspark.ml.evaluation import (\n",
    "    BinaryClassificationEvaluator,\n",
    "    RegressionEvaluator,\n",
    ")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1781597556876,
     "inputWidgets": {},
     "nuid": "228f700d-9bcc-4670-a91d-df9afb07142c",
     "showTitle": true,
     "startTime": 1781597556668,
     "submitTime": 1781597169294,
     "tableResultSettingsMap": {},
     "title": "Spark Performance"
    }
   },
   "outputs": [],
   "source": [
    "# Spark Performance Settings\n",
    "spark.conf.set(\"spark.sql.shuffle.partitions\", \"auto\")\n",
    "spark.conf.set(\"spark.sql.adaptive.enabled\", \"true\")\n",
    "spark.conf.set(\"spark.sql.execution.arrow.pyspark.enabled\", \"true\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "7f752e07-bb3f-4502-9528-cf4311a9d673",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## General Functions"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1782125835882,
     "inputWidgets": {},
     "nuid": "e618f44e-2527-4562-b985-162ada746600",
     "showTitle": true,
     "startTime": 1782125835792,
     "submitTime": 1782125834847,
     "tableResultSettingsMap": {},
     "title": "Widget Functions"
    }
   },
   "outputs": [],
   "source": [
    "def get_widget_value(name, default):\n",
    "    try:\n",
    "        dbutils.widgets.text(name, str(default))\n",
    "        value = dbutils.widgets.get(name)\n",
    "        return value if value not in (None, \"\") else default\n",
    "    except NameError:\n",
    "        return default\n",
    "\n",
    "\n",
    "def validate_positive_int(name, value):\n",
    "    parsed_value = int(value)\n",
    "    if parsed_value <= 0:\n",
    "        raise ValueError(\n",
    "            f\"Invalid widget value for {name}: {value}. Value must be a positive integer.\"\n",
    "        )\n",
    "    return parsed_value"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1782125836466,
     "inputWidgets": {},
     "nuid": "5baab63b-1fa3-4b1a-9bb5-fcd44f8eeda1",
     "showTitle": true,
     "startTime": 1782125836035,
     "submitTime": 1782125836009,
     "tableResultSettingsMap": {},
     "title": "Environment set up"
    }
   },
   "outputs": [],
   "source": [
    "dbutils.widgets.text(\n",
    "    name=\"catalog_schema_prefix\",\n",
    "    defaultValue=\"marketingdata_dev.claire_wilsonbarnes\",\n",
    "    label=\"catalog_schema_prefix\",\n",
    ")\n",
    "dbutils.widgets.text(\n",
    "    name=\"table_prefix\",\n",
    "    defaultValue=\"next_uk_nextAds_analytics_pctr\",\n",
    "    label=\"table_prefix\",\n",
    ")\n",
    "\n",
    "dbutils.widgets.text(\n",
    "    name=\"lookback_period\", defaultValue=\"30\", label=\"lookback_period\"\n",
    ")\n",
    "\n",
    "catalog_schema_prefix = get_widget_value(\n",
    "    \"catalog_schema_prefix\", \"marketingdata_dev.claire_wilsonbarnes\"\n",
    ")\n",
    "lookback_period = int(get_widget_value(\"lookback_period\", \"30\"))\n",
    "table_prefix = get_widget_value(\n",
    "    \"table_prefix\", \"next_uk_nextAds_analytics_pctr\"\n",
    ")\n",
    "\n",
    "## mlflow\n",
    "experiment_path = \"/Workspace/Users/claire_wilsonbarnes@next.co.uk/mlflow/nextads/dev/experiments/analytics_pctr_model\"\n",
    "mlflow.set_tracking_uri(\"databricks\")\n",
    "mlflow.set_registry_uri(\"databricks-uc\")\n",
    "mlflow.set_experiment(experiment_path)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "7078a009-64dd-4b09-b956-1584c341247d",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## Variables"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1782125973387,
     "inputWidgets": {},
     "nuid": "d10f3ac8-8384-41de-9d9f-a2cbac35f869",
     "showTitle": true,
     "startTime": 1782125973275,
     "submitTime": 1782125973181,
     "tableResultSettingsMap": {},
     "title": "Parameters"
    }
   },
   "outputs": [],
   "source": [
    "## Training Data parameters\n",
    "training_data_table = (\n",
    "    f\"{catalog_schema_prefix}.{table_prefix}_training_history\"\n",
    ")\n",
    "final_validation_data_table = (\n",
    "    f\"{catalog_schema_prefix}.{table_prefix}_validation_history\"\n",
    ")\n",
    "\n",
    "classifier_registered_model_name = \"marketingdata_dev.ds_sandbox.nextads_analytics_pctr_popularity_classification_model\"\n",
    "regresssor_registered_model_name = \"marketingdata_dev.ds_sandbox.nextads_analytics_pctr_affinity_regression_model\"\n",
    "\n",
    "# Due to missing click data can ONLY use data from '2026-04-29' onwards currently ( OR use data prior to 19/02/2026) to ensure no skew in the metrics being utlised over the 30 day time window\n",
    "start_date_filter = \"2026-04-29\"\n",
    "# Sample via timeseries\n",
    "# split into 3 datasets- training, validation and test\n",
    "# Final Validation = 1st-7th June ( with ALL adverts for all customers)\n",
    "train_cutoff_date = \"2026-05-23\"\n",
    "validation_cutoff_date = \"2026-05-29\"\n",
    "\n",
    "# Sample weighting parameters\n",
    "# Add more weight to a Basic reccomendation that the Best reccomendations\n",
    "# Approx 10% of ads get basic targeting\n",
    "advert_proportions = {\"Basic\": 0.1, \"Best\": 0.9}\n",
    "# To dampen the impact & reduce variance - might need to tune this!\n",
    "alpha = 0.5\n",
    "random_seed = 553\n",
    "\n",
    "\n",
    "primary_keys = [\"account_number\", \"uniqueAdID\", \"rundate\"]\n",
    "\n",
    "fill_zeros_columns = {\n",
    "    \"day_impressions\": 0,\n",
    "    \"prior_day_impressions\": 0,\n",
    "    \"week_impressions\": 0,\n",
    "    \"prior_week_impressions\": 0,\n",
    "    \"view_highest_catid_weight\": 0,\n",
    "    \"view_lift_adjusted\": 0,\n",
    "    \"view_cs\": 0,\n",
    "    \"purchase_highest_catid_weight\": 0,\n",
    "    \"purchase_lift_adjusted\": 0,\n",
    "    \"purchase_cs\": 0,\n",
    "}\n",
    "\n",
    "imputation_columns = {\"age\": \"age_imputed\"}\n",
    "\n",
    "non_feature_cols = [\n",
    "    \"potnumber\",\n",
    "    \"campaignnumber\",\n",
    "    # Included for potential  pre-filtering for low visibility ads\n",
    "    \"total_ad_impressions\",\n",
    "    \"total_ad_perc_impressions\",\n",
    "    \"total_ad_cumulativeperc_impresssions\",\n",
    "    \"gender\",\n",
    "    \"postcodearea\",\n",
    "    \"primary_advert_location\",\n",
    "    \"view_support12\",\n",
    "    \"view_support1\",\n",
    "    \"view_lift\",\n",
    "    \"purchase_support12\",\n",
    "    \"purchase_support1\",\n",
    "    \"purchase_support2\",\n",
    "    \"pruchase_lift\",\n",
    "    \"treatment_typelocation\",\n",
    "]\n",
    "removed_feature_cols = [\n",
    "    \"mailoptout\",\n",
    "    \"staff_indicator\",\n",
    "    \"channel_ctr\",\n",
    "    \"dayofweek_ctr\",\n",
    "    \"perc_viewtimedow\",\n",
    "    \"customer_lifespan\",\n",
    "    \"total_time_on_site\",\n",
    "    \"total_sessions\",\n",
    "    \"avg_site_time\",\n",
    "    \"med_time_onsite\",\n",
    "    \"number_departments_viewed\",\n",
    "    \"number_pages_viewed_last_week\",\n",
    "    \"total_order_value\",\n",
    "    \"total_number_items\",\n",
    "    \"customer_total_impressions\",\n",
    "    \"customer_total_unique_adverts\",\n",
    "    \"customer_advert_previous_impression_number\",\n",
    "    \"number_algodivisions_clicked\",\n",
    "    \"number_algodivisions_impressions\",\n",
    "    \"number_impressions_same_algodivision\",\n",
    "    \"number_unique_adverts_same_algodivision\",\n",
    "    \"number_unique_adverts_clicked_same_algodivision\",\n",
    "    \"channel_impressions\",\n",
    "    \"dayofweek_impressions\",\n",
    "    \"week_impressions\",\n",
    "    \"prior_week_impressions\",\n",
    "    \"highest_spend_cat_alignment\",\n",
    "    \"view_cs\",\n",
    "    \"purchase_cs\",\n",
    "]\n",
    "\n",
    "popularity_feature_columns = [\n",
    "    \"cash_acc\",\n",
    "    \"advert_ctr\",\n",
    "    \"device_ctr\",\n",
    "    \"geo_ctr\",\n",
    "    \"gender_ctr\",\n",
    "    \"dod_ctr_change\",\n",
    "    \"wow_ctr_change\",\n",
    "    \"age_imputed\",\n",
    "    \"number_pages_viewed\",\n",
    "    \"prior_30_day_order_value\",\n",
    "    \"customer_total_clicks\",\n",
    "    \"customer_total_unique_adverts_clicked\",\n",
    "    \"customer_advert_previous_click_number\",\n",
    "    \"number_clicks_same_algodivision\",\n",
    "    \"advert_impressions\",\n",
    "    \"device_impressions\",\n",
    "    \"geo_impressions\",\n",
    "    \"gender_impressions\",\n",
    "    \"day_impressions\",\n",
    "    \"prior_day_impressions\",\n",
    "]\n",
    "\n",
    "\n",
    "affinity_feature_columns = [\n",
    "    \"view_theme_score\",\n",
    "    \"perc_order_value_cat_affinity\",\n",
    "    \"perc_30_day_order_value_cat_affinity\",\n",
    "    \"perc_order_qty_cat_affinity\",\n",
    "    \"view_highest_catid_weight\",\n",
    "    \"view_lift_adjusted\",\n",
    "    \"purchase_highest_catid_weight\",\n",
    "    \"purchase_lift_adjusted\",\n",
    "    \"purchase_theme_affinity\",\n",
    "]\n",
    "\n",
    "### Column naming parameters\n",
    "## Popularity Classifier\n",
    "popularity_target_col = \"ad_clicked\"\n",
    "weighting_column = \"sample_weight\"\n",
    "popularity_prediction_col = \"prediction\"\n",
    "popularity_probability_col = \"probability\"\n",
    "popularity_raw_prediction_col = \"rawPrediction\"\n",
    "popularity_vector_assembler_features = \"features\"\n",
    "popularity_click_prob_col = \"popularity_prob_click\"\n",
    "\n",
    "# Affinity Regressor\n",
    "regressor_vector_assembler_features = \"regressor_features\"\n",
    "regressor_target_col = \"residuals\"\n",
    "regressor_predictions_col = \"residual_predictions\"\n",
    "\n",
    "## Ranking\n",
    "popularity_smoothed_score_col = \"popularity_smoothed_score\"\n",
    "regression_weighted_score_col = \"regression_weighted_score\"\n",
    "combined_score_col = \"combined_weighted_score\"\n",
    "weighted_ranking_col = \"weighted_ranking\""
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1782125938361,
     "inputWidgets": {},
     "nuid": "747ae44a-c48e-4a3c-915c-375dde27f696",
     "showTitle": true,
     "startTime": 1782125938212,
     "submitTime": 1782125938184,
     "tableResultSettingsMap": {},
     "title": "Model Components"
    }
   },
   "outputs": [],
   "source": [
    "## Pipeline components\n",
    "# Preprocessing Popularity Model\n",
    "# Inmputing age\n",
    "imputer = Imputer(\n",
    "    strategy=\"median\",\n",
    "    inputCols=list(imputation_columns.keys()),\n",
    "    outputCols=list(imputation_columns.values()),\n",
    ")\n",
    "popularity_vector_assembler = VectorAssembler(\n",
    "    inputCols=popularity_feature_columns,\n",
    "    outputCol=popularity_vector_assembler_features,\n",
    ")\n",
    "\n",
    "## Affinity Regressor Model\n",
    "regressor_vector_assembler = VectorAssembler(\n",
    "    inputCols=affinity_feature_columns,\n",
    "    outputCol=regressor_vector_assembler_features,\n",
    ")\n",
    "models_dict = {\n",
    "    \"popularity_classifier\": [\n",
    "        {\n",
    "            \"model_name\": \"spark_xgboost_classifier\",\n",
    "            \"estimator\": SparkXGBClassifier(\n",
    "                features_col=popularity_vector_assembler_features,\n",
    "                label_col=popularity_target_col,\n",
    "                prediction=popularity_prediction_col,\n",
    "                raw_prediction=popularity_raw_prediction_col,\n",
    "                probability=popularity_probability_col,\n",
    "                weight_col=weighting_column,\n",
    "                seed=random_seed,\n",
    "            ),\n",
    "            \"params\": {\n",
    "                \"max_depth\": 6,\n",
    "                \"n_estimators\": 200,\n",
    "                \"learning_rate\": 0.05,\n",
    "                \"eval_metric\": \"aucpr\",\n",
    "                \"colsample_bytree\": 0.4,\n",
    "                \"min_child_weight\": 1,\n",
    "            },\n",
    "        }\n",
    "    ],\n",
    "    \"affinity_regressor\": [\n",
    "        {\n",
    "            \"model_name\": \"spark_xgboost_regressor\",\n",
    "            \"estimator\": SparkXGBRegressor(\n",
    "                features_col=regressor_vector_assembler_features,\n",
    "                label_col=regressor_target_col,\n",
    "                prediction_col=regressor_predictions_col,\n",
    "                weight_col=weighting_column,\n",
    "                seed=random_seed,\n",
    "            ),\n",
    "            \"params\": {\n",
    "                \"max_depth\": 4,\n",
    "                \"n_estimators\": 200,\n",
    "                \"learning_rate\": 0.05,\n",
    "                \"eval_metric\": \"rmse\",\n",
    "                \"colsample_bytree\": 1,\n",
    "                \"min_child_weight\": 1,\n",
    "            },\n",
    "        }\n",
    "    ],\n",
    "}"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1782125868805,
     "inputWidgets": {},
     "nuid": "fc6d43eb-c079-45b2-bf97-03ea05d31d23",
     "showTitle": true,
     "startTime": 1782125868527,
     "submitTime": 1782125868498,
     "tableResultSettingsMap": {},
     "title": "Evaluators"
    }
   },
   "outputs": [],
   "source": [
    "### Popularity Classifier Model Evaluators\n",
    "# Training Evaluators build\n",
    "areaunderPR_eval = BinaryClassificationEvaluator(\n",
    "    labelCol=popularity_target_col,\n",
    "    metricName=\"areaUnderPR\",\n",
    "    weightCol=weighting_column,\n",
    "    rawPredictionCol=popularity_raw_prediction_col,\n",
    ")\n",
    "areaunderroc_eval = BinaryClassificationEvaluator(\n",
    "    labelCol=popularity_target_col,\n",
    "    metricName=\"areaUnderROC\",\n",
    "    rawPredictionCol=popularity_raw_prediction_col,\n",
    "    weightCol=weighting_column,\n",
    ")\n",
    "## Validation Evaluators Build\n",
    "unweighted_areaunderPR_eval = BinaryClassificationEvaluator(\n",
    "    labelCol=popularity_target_col,\n",
    "    metricName=\"areaUnderPR\",\n",
    "    rawPredictionCol=popularity_raw_prediction_col,\n",
    ")\n",
    "unweighted_areaunderroc_eval = BinaryClassificationEvaluator(\n",
    "    labelCol=popularity_target_col,\n",
    "    metricName=\"areaUnderROC\",\n",
    "    rawPredictionCol=popularity_raw_prediction_col,\n",
    ")\n",
    "\n",
    "### Affinity Regressor Model Evaluators\n",
    "rmse_eval = RegressionEvaluator(\n",
    "    labelCol=regressor_target_col,\n",
    "    predictionCol=regressor_predictions_col,\n",
    "    metricName=\"rmse\",\n",
    ")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "c7fcd529-233c-46f8-8769-d2989b0b1a40",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## Model Functions"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1782125872808,
     "inputWidgets": {},
     "nuid": "7d4fccab-3372-41ac-a17c-f6964c466e01",
     "showTitle": true,
     "startTime": 1782125872627,
     "submitTime": 1782125872600,
     "tableResultSettingsMap": {},
     "title": "Sample Weighting Function"
    }
   },
   "outputs": [],
   "source": [
    "def create_sample_weight(df, input_col, output_col, percentile_split, alpha):\n",
    "\n",
    "    if len(percentile_split) != 2:\n",
    "        print(\n",
    "            f\"Error expected 2 values for the percentile split but recieved {len(percentile_split)}\"\n",
    "        )\n",
    "        df = df.withColumn(output_col, F.lit(1))\n",
    "        return df\n",
    "\n",
    "    percentile_keys = list(percentile_split.keys())\n",
    "    percentile_values = list(percentile_split.values())\n",
    "    df = df.withColumn(\n",
    "        \"raw_weight\",\n",
    "        F.when(\n",
    "            F.col(input_col) == percentile_keys[0], 1 / percentile_values[0]\n",
    "        ).otherwise(1 / percentile_values[1]),\n",
    "    ).withColumn(\"weight\", F.pow(F.col(\"raw_weight\"), F.lit(alpha)))\n",
    "    weight_sum, weight_count = df.agg(\n",
    "        F.sum(\"weight\"), F.count(\"weight\")\n",
    "    ).collect()[0]\n",
    "    df = df.withColumn(\n",
    "        \"sample_weight\", F.col(\"weight\") * F.lit(weight_count / weight_sum)\n",
    "    )\n",
    "    return df"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1782125874810,
     "inputWidgets": {},
     "nuid": "1b22d2af-5c07-4180-bafb-ab691cf56d5a",
     "showTitle": true,
     "startTime": 1782125874688,
     "submitTime": 1782125874660,
     "tableResultSettingsMap": {},
     "title": "Evaluator Functions"
    }
   },
   "outputs": [],
   "source": [
    "def classification_eval(df, include_weighting=True):\n",
    "\n",
    "    auPR = unweighted_areaunderPR_eval.evaluate(df)\n",
    "    auroc = unweighted_areaunderroc_eval.evaluate(df)\n",
    "    results = {\n",
    "        \"auPR\": auPR,\n",
    "        \"auROC\": auroc,\n",
    "    }\n",
    "    if include_weighting:\n",
    "        auPRweighted = areaunderPR_eval.evaluate(df)\n",
    "        aurocweighted = areaunderroc_eval.evaluate(df)\n",
    "        results[\"auPR_weighted\"] = auPRweighted\n",
    "        results[\"auROC_weighted\"] = aurocweighted\n",
    "    return results\n",
    "\n",
    "\n",
    "def regression_eval(df):\n",
    "    rmse = rmse_eval.evaluate(df)\n",
    "    return {\"rmse\": rmse}"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1782125876911,
     "inputWidgets": {},
     "nuid": "1058e06e-e5bf-4ae3-9f47-00f9d26eee06",
     "showTitle": true,
     "startTime": 1782125876733,
     "submitTime": 1782125876632,
     "tableResultSettingsMap": {},
     "title": "Model Training Functionality"
    }
   },
   "outputs": [],
   "source": [
    "def run_model_training(\n",
    "    model_type: str,\n",
    "    model_specs: list,\n",
    "    train_df,\n",
    "    val_df,\n",
    "    preprocessing_steps: list,\n",
    "    target_col,\n",
    "    label_col,\n",
    "    final_feature_cols,\n",
    "    random_seed,\n",
    "    split_df_summary,\n",
    "    weighted_model: bool = False,\n",
    "    targeting_details={},\n",
    "):\n",
    "\n",
    "    print(\"Starting Model Training\")\n",
    "    best_model_run_id = None\n",
    "    best_model_estimator_score = 0\n",
    "\n",
    "    for spec in model_specs:\n",
    "        current_result = None\n",
    "        run_timestamp = datetime.now().strftime(\"%Y%m%d_%H%M%S\")\n",
    "        model_name = spec.get(\"model_name\")\n",
    "        estimator = spec.get(\"estimator\")\n",
    "        estimator.setParams(**spec.get(\"params\"))\n",
    "        pipeline = Pipeline(stages=preprocessing_steps + [estimator])\n",
    "        run_name = f\"analytics_pctr_{model_name}_{run_timestamp}\"\n",
    "        print(f\"Running Model {run_name}\")\n",
    "        try:\n",
    "            with mlflow.start_run(run_name=run_name) as run:\n",
    "                print(f\"Training Model {model_name}\")\n",
    "                mlflow.log_param(\"model_name\", model_name)\n",
    "                mlflow.log_param(\"input_table\", train_df)\n",
    "                mlflow.log_param(\"target_col\", target_col)\n",
    "                mlflow.log_param(\"label_col\", label_col)\n",
    "                mlflow.log_param(\"seed\", random_seed)\n",
    "                mlflow.log_param(\"feature_count\", len(final_feature_cols))\n",
    "                if weighted_model:\n",
    "                    mlflow.log_params(\n",
    "                        {\n",
    "                            f\"weighting__{key}\": value\n",
    "                            for key, value in targeting_details.items()\n",
    "                        }\n",
    "                    )\n",
    "                mlflow.log_params(\n",
    "                    {\n",
    "                        f\"model__{key}\": value\n",
    "                        for key, value in spec.get(\"params\").items()\n",
    "                    }\n",
    "                )\n",
    "                mlflow.log_text(\n",
    "                    json.dumps(final_feature_cols, indent=2),\n",
    "                    \"feature_columns.json\",\n",
    "                )\n",
    "\n",
    "                mlflow.log_table(\n",
    "                    split_df_summary.toPandas(),\n",
    "                    artifact_file=\"train_val_test_splits.json\",\n",
    "                )\n",
    "                # Train pipeline\n",
    "                print(f\"Training pipeline for {model_type} {model_name}\")\n",
    "                trained_model = pipeline.fit(train_df)\n",
    "                # Get training metrics\n",
    "                pred_train = trained_model.transform(train_df)\n",
    "                pred_val = trained_model.transform(val_df)\n",
    "\n",
    "                if model_type == \"popularity_classifier\":\n",
    "                    train_eval = classification_eval(pred_train)\n",
    "                    for metric, value in train_eval.items():\n",
    "                        mlflow.log_metric(f\"train_{metric}\", value)\n",
    "                    # Get validation metrics\n",
    "                    val_eval = classification_eval(pred_val)\n",
    "                    for metric, value in val_eval.items():\n",
    "                        mlflow.log_metric(f\"val_{metric}\", value)\n",
    "                    current_result = (\n",
    "                        val_eval[\"auPR_weighted\"]\n",
    "                        if weighted_model\n",
    "                        else val_eval[\"auPR\"]\n",
    "                    )\n",
    "                    if current_result > best_model_estimator_score:\n",
    "                        best_model_estimator_score = current_result\n",
    "                        best_model_run_id = run.info.run_id\n",
    "\n",
    "                elif model_type == \"affinity_regressor\":\n",
    "                    # Training metrics:\n",
    "                    train_eval = regression_eval(pred_train)\n",
    "                    for metric, value in train_eval.items():\n",
    "                        mlflow.log_metric(f\"train_{metric}\", value)\n",
    "                    # Validation metrics:\n",
    "                    val_eval = regression_eval(pred_val)\n",
    "                    for metric, value in val_eval.items():\n",
    "                        mlflow.log_metric(f\"val_{metric}\", value)\n",
    "\n",
    "                    current_result = val_eval[\"rmse\"]\n",
    "\n",
    "                    if (\n",
    "                        current_result < best_model_estimator_score\n",
    "                        or best_model_estimator_score == 0\n",
    "                    ):\n",
    "                        best_model_estimator_score = current_result\n",
    "                        best_model_run_id = run.info.run_id\n",
    "                else:\n",
    "                    print(f\"Error - unknown model type {model_type}\")\n",
    "                signature = infer_signature(\n",
    "                    model_input=train_df.sample(\n",
    "                        fraction=0.01, seed=random_seed\n",
    "                    ),\n",
    "                    model_output=pred_train.sample(\n",
    "                        fraction=0.01, seed=random_seed\n",
    "                    ),\n",
    "                )\n",
    "                mlflow.spark.log_model(\n",
    "                    trained_model, artifact_path=\"model\", signature=signature\n",
    "                )\n",
    "        except Exception as e:\n",
    "            print(f\"Error training model {model_name}: {e}\")\n",
    "            mlflow.end_run()\n",
    "    return best_model_run_id"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "a466091d-4a1b-40a7-97eb-337aaef633ac",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## Training\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1782125880014,
     "inputWidgets": {},
     "nuid": "04b2647e-7ab5-444a-bef8-fd41b11421c4",
     "showTitle": true,
     "startTime": 1782125879545,
     "submitTime": 1782125879510,
     "tableResultSettingsMap": {},
     "title": "Training data"
    }
   },
   "outputs": [],
   "source": [
    "# Core training dataset\n",
    "\n",
    "df = spark.table(training_data_table)\n",
    "# Preprocessing\n",
    "# Setting a hard limit of start date due to data issues - will be a non-issue for future\n",
    "df = df.filter(F.col(\"rundate\") >= start_date_filter)\n",
    "df = df.fillna(fill_zeros_columns)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1782125898227,
     "inputWidgets": {},
     "nuid": "3dc87b3e-0cf1-48fd-93a0-bac5982f2867",
     "showTitle": true,
     "startTime": 1782125881600,
     "submitTime": 1782125881572,
     "tableResultSettingsMap": {},
     "title": "Train/Val /Test Splits"
    }
   },
   "outputs": [],
   "source": [
    "df = df.withColumn(\n",
    "    \"split_type\",\n",
    "    F.when(F.col(\"rundate\") < train_cutoff_date, \"train\")\n",
    "    .when(F.col(\"rundate\") >= validation_cutoff_date, \"test\")\n",
    "    .otherwise(\"validation\"),\n",
    ")\n",
    "\n",
    "# Split of datasets for training\n",
    "train_df = df.filter(F.col(\"split_type\") == \"train\")\n",
    "validation_df = df.filter((F.col(\"split_type\") == \"validation\"))\n",
    "test_df = df.filter(F.col(\"split_type\") == \"test\")\n",
    "\n",
    "# View of splits\n",
    "split_df_summary = (\n",
    "    df.groupBy(\"split_type\")\n",
    "    .agg(\n",
    "        F.count(\"*\").alias(\"records\"),\n",
    "        F.countDistinct(\"account_number\").alias(\"accounts\"),\n",
    "        F.sum(\"ad_clicked\").alias(\"positive_rows\"),\n",
    "    )\n",
    "    .withColumn(\n",
    "        \"percent_positive\",\n",
    "        F.round(F.col(\"positive_rows\") / F.col(\"records\"), 4),\n",
    "    )\n",
    ")\n",
    "display(split_df_summary)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1782125899934,
     "inputWidgets": {},
     "nuid": "1f23ae63-007f-4ab3-af50-735a219b0694",
     "showTitle": true,
     "startTime": 1782125898235,
     "submitTime": 1782125884743,
     "tableResultSettingsMap": {},
     "title": "Creation of sample weights for training"
    }
   },
   "outputs": [],
   "source": [
    "# Use Treatment type as sample weighting for training (addition to sampel sets for validation metrics)\n",
    "train_df = create_sample_weight(\n",
    "    train_df, \"treatment_type\", \"sample_weight\", advert_proportions, alpha\n",
    ")\n",
    "validation_df = create_sample_weight(\n",
    "    validation_df, \"treatment_type\", \"sample_weight\", advert_proportions, alpha\n",
    ")\n",
    "test_df = create_sample_weight(\n",
    "    test_df, \"treatment_type\", \"sample_weight\", advert_proportions, alpha\n",
    ")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1782126108199,
     "inputWidgets": {},
     "nuid": "4b79e873-293d-46b9-bbfc-23ff9b559da8",
     "showTitle": true,
     "startTime": 1782125979796,
     "submitTime": 1782125979768,
     "tableResultSettingsMap": {},
     "title": "Classification Model Training"
    }
   },
   "outputs": [],
   "source": [
    "# Train the Classifier:\n",
    "\n",
    "classifier_model_run_id = run_model_training(\n",
    "    \"popularity_classifier\",\n",
    "    models_dict.get(\"popularity_classifier\", []),\n",
    "    train_df,\n",
    "    validation_df,\n",
    "    [imputer, popularity_vector_assembler],\n",
    "    popularity_target_col,\n",
    "    popularity_probability_col,\n",
    "    popularity_feature_columns,\n",
    "    random_seed,\n",
    "    split_df_summary,\n",
    "    True,\n",
    "    advert_proportions | {\"alpha\": alpha},\n",
    ")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1782126121713,
     "inputWidgets": {},
     "nuid": "edcef145-8484-47b1-ae52-6c5aecea50cb",
     "showTitle": true,
     "startTime": 1782126108219,
     "submitTime": 1782125993570,
     "tableResultSettingsMap": {},
     "title": "Data preparation for Regressor Model"
    }
   },
   "outputs": [],
   "source": [
    "## Create the training set for the Regressor:\n",
    "popularity_model = mlflow.spark.load_model(\n",
    "    f\"runs:/{classifier_model_run_id}/model\"\n",
    ")\n",
    "train_df_popularity_scored = popularity_model.transform(train_df)\n",
    "val_df_popularity_scored = popularity_model.transform(validation_df)\n",
    "\n",
    "# Calculate the residuals for the Regressor model\n",
    "train_df_popularity_scored = train_df_popularity_scored.withColumn(\n",
    "    popularity_click_prob_col,\n",
    "    vector_to_array(F.col(popularity_probability_col))[1],\n",
    ").withColumn(\n",
    "    regressor_target_col,\n",
    "    F.col(popularity_target_col) - F.col(popularity_click_prob_col),\n",
    ")\n",
    "val_df_popularity_scored = val_df_popularity_scored.withColumn(\n",
    "    popularity_click_prob_col,\n",
    "    vector_to_array(F.col(popularity_probability_col))[1],\n",
    ").withColumn(\n",
    "    regressor_target_col,\n",
    "    F.col(popularity_target_col) - F.col(popularity_click_prob_col),\n",
    ")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1782126193766,
     "inputWidgets": {},
     "nuid": "a9619866-426f-426c-8a4c-350b61d72391",
     "showTitle": true,
     "startTime": 1782126121726,
     "submitTime": 1782125995852,
     "tableResultSettingsMap": {},
     "title": "Regressor Model Training"
    }
   },
   "outputs": [],
   "source": [
    "regressor_model_run_id = run_model_training(\n",
    "    \"affinity_regressor\",\n",
    "    models_dict.get(\"affinity_regressor\", []),\n",
    "    train_df_popularity_scored,\n",
    "    val_df_popularity_scored,\n",
    "    [regressor_vector_assembler],\n",
    "    regressor_target_col,\n",
    "    regressor_predictions_col,\n",
    "    affinity_feature_columns,\n",
    "    random_seed,\n",
    "    split_df_summary,\n",
    "    True,\n",
    ")\n",
    "\n",
    "affinity_model = mlflow.spark.load_model(\n",
    "    f\"runs:/{regressor_model_run_id}/model\"\n",
    ")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1782126202473,
     "inputWidgets": {},
     "nuid": "fc6f22e1-3826-4e3c-b30b-e0e764a5143a",
     "showTitle": true,
     "startTime": 1782126193774,
     "submitTime": 1782125997548,
     "tableResultSettingsMap": {},
     "title": "Test the Best regressor and Estimator on the training set"
    }
   },
   "outputs": [],
   "source": [
    "# Run test model scoring\n",
    "test_df_popularity_scored = popularity_model.transform(test_df)\n",
    "test_eval = classification_eval(test_df_popularity_scored)\n",
    "for metric, value in test_eval.items():\n",
    "    mlflow.log_metric(f\"test_{metric}\", value, run_id=classifier_model_run_id)\n",
    "\n",
    "# Calculate the residuals for the Regressor model\n",
    "test_df_popularity_scored = test_df_popularity_scored.withColumn(\n",
    "    popularity_click_prob_col,\n",
    "    vector_to_array(F.col(popularity_probability_col))[1],\n",
    ").withColumn(\n",
    "    regressor_target_col,\n",
    "    F.col(popularity_target_col) - F.col(popularity_click_prob_col),\n",
    ")\n",
    "\n",
    "# Run affinity model scoring\n",
    "test_df_affinity_scored = affinity_model.transform(test_df_popularity_scored)\n",
    "test_eval_reg = regression_eval(test_df_affinity_scored)\n",
    "for metric, value in test_eval_reg.items():\n",
    "    mlflow.log_metric(f\"test_{metric}\", value, run_id=regressor_model_run_id)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1782126315156,
     "inputWidgets": {},
     "nuid": "c9fd2682-4244-495c-94de-a9b0a7b0920c",
     "showTitle": true,
     "startTime": 1782126230331,
     "submitTime": 1782126230248,
     "tableResultSettingsMap": {},
     "title": "Run final Validation model predictions"
    }
   },
   "outputs": [],
   "source": [
    "# Run final validation model processing\n",
    "## Validation section with ALL adverts for all customers\n",
    "final_validation_df = spark.table(final_validation_data_table)\n",
    "final_validation_df = final_validation_df.fillna(fill_zeros_columns)\n",
    "\n",
    "\n",
    "final_validation_df_popularity_scored = popularity_model.transform(\n",
    "    final_validation_df\n",
    ")\n",
    "final_val_eval = classification_eval(\n",
    "    final_validation_df_popularity_scored.filter(\n",
    "        F.col(popularity_target_col).isNotNull()\n",
    "    ),\n",
    "    False,\n",
    ")\n",
    "for metric, value in final_val_eval.items():\n",
    "    mlflow.log_metric(\n",
    "        f\"final_validation_{metric}\", value, run_id=classifier_model_run_id\n",
    "    )\n",
    "\n",
    "# Calculate the residuals for the Regressor model\n",
    "final_validation_df_popularity_scored = (\n",
    "    final_validation_df_popularity_scored.withColumn(\n",
    "        popularity_click_prob_col,\n",
    "        vector_to_array(F.col(popularity_probability_col))[1],\n",
    "    ).withColumn(\n",
    "        regressor_target_col,\n",
    "        F.col(popularity_target_col) - F.col(popularity_click_prob_col),\n",
    "    )\n",
    ")\n",
    "\n",
    "# Run affinity model scoring\n",
    "final_validation_df_affinity_scored = affinity_model.transform(\n",
    "    final_validation_df_popularity_scored\n",
    ")\n",
    "final_val_eval_reg = regression_eval(\n",
    "    final_validation_df_affinity_scored.filter(\n",
    "        F.col(popularity_target_col).isNotNull()\n",
    "    )\n",
    ")\n",
    "for metric, value in final_val_eval_reg.items():\n",
    "    mlflow.log_metric(\n",
    "        f\"final_validation__{metric}\", value, run_id=regressor_model_run_id\n",
    "    )\n",
    "\n",
    "## cache the dataset\n",
    "final_validation_df_affinity_scored.cache()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1782126510919,
     "inputWidgets": {},
     "nuid": "b250c6fd-a59e-458b-9211-077207360550",
     "showTitle": true,
     "startTime": 1782126315181,
     "submitTime": 1782126234011,
     "tableResultSettingsMap": {},
     "title": "write out & reimport the final validation set"
    }
   },
   "outputs": [],
   "source": [
    "final_validation_results_table = (\n",
    "    catalog_schema_prefix + \".pctr_final_validation_predictions\"\n",
    ")\n",
    "final_validation_df_affinity_scored.write.mode(\"overwrite\").format(\n",
    "    \"delta\"\n",
    ").saveAsTable(final_validation_results_table)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "5e36ad75-5e96-433c-b852-71efdc290ff7",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## Feature Importance"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1782126511018,
     "inputWidgets": {},
     "nuid": "b9b68484-34cd-4209-aa4c-7b937b977d9b",
     "showTitle": true,
     "startTime": 1782126510927,
     "submitTime": 1782126238170,
     "tableResultSettingsMap": {},
     "title": "Feature Importance for best models"
    }
   },
   "outputs": [],
   "source": [
    "# Feature importance\n",
    "def feature_imortances_append_names(importances, feature_names):\n",
    "    cleaned_importance = {}\n",
    "    for f_index, score in importances.items():\n",
    "        # Extract the integer from 'f0', 'f1', etc.\n",
    "        idx = int(f_index.replace(\"f\", \"\"))\n",
    "        # Look up the actual column name\n",
    "        actual_name = feature_names[idx]\n",
    "        cleaned_importance[actual_name] = score\n",
    "    df_importance = pd.DataFrame(\n",
    "        list(cleaned_importance.items()), columns=[\"Feature\", \"Importance\"]\n",
    "    ).sort_values(by=\"Importance\", ascending=False)\n",
    "\n",
    "    return df_importance\n",
    "\n",
    "\n",
    "affinity_feature_importances = feature_imortances_append_names(\n",
    "    affinity_model.stages[-1].get_booster().get_score(importance_type=\"gain\"),\n",
    "    affinity_model.stages[-2].getInputCols(),\n",
    ")\n",
    "popularity_feature_importances = feature_imortances_append_names(\n",
    "    popularity_model.stages[-1]\n",
    "    .get_booster()\n",
    "    .get_score(importance_type=\"gain\"),\n",
    "    popularity_model.stages[-2].getInputCols(),\n",
    ")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1782126511519,
     "inputWidgets": {},
     "nuid": "f4d793e2-d4d5-4e1b-8e26-339993247b42",
     "showTitle": true,
     "startTime": 1782126511024,
     "submitTime": 1782126240328,
     "tableResultSettingsMap": {},
     "title": "Popularity Model Feature Importances"
    }
   },
   "outputs": [],
   "source": [
    "plt.figure(figsize=(10, 10))\n",
    "sns.barplot(x=\"Importance\", y=\"Feature\", data=popularity_feature_importances)\n",
    "plt.title(\"Popularity Feature Importance\")\n",
    "plt.xlabel"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1782126511920,
     "inputWidgets": {},
     "nuid": "e9719732-07f8-4550-aab3-7c363ec7a592",
     "showTitle": true,
     "startTime": 1782126511526,
     "submitTime": 1782126242291,
     "tableResultSettingsMap": {},
     "title": "Affinity Model Feature Importances"
    }
   },
   "outputs": [],
   "source": [
    "plt.figure(figsize=(10, 10))\n",
    "sns.barplot(x=\"Importance\", y=\"Feature\", data=affinity_feature_importances)\n",
    "plt.title(\"Affinity Feature Importance\")\n",
    "plt.xlabel"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "6443ed83-5213-4e07-ad0b-8e046400abbe",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## Ranking Evaluation Functions "
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1782126512020,
     "inputWidgets": {},
     "nuid": "07ffe558-afc5-4107-bdac-1ce3e549fc00",
     "showTitle": true,
     "startTime": 1782126511925,
     "submitTime": 1782126245354,
     "tableResultSettingsMap": {},
     "title": "Lift above n"
    }
   },
   "outputs": [],
   "source": [
    "def rank_n_and_above_lift_metrics(df, n=2):\n",
    "    ranking_above_col = f\"rank_above_{n}\"\n",
    "    top_n_ctr = (\n",
    "        df.filter(F.col(popularity_target_col).isNotNull())\n",
    "        .withColumn(ranking_above_col, F.col(weighted_ranking_col) <= n)\n",
    "        .groupBy(F.col(ranking_above_col))\n",
    "        .agg(\n",
    "            F.sum(F.col(popularity_target_col)).alias(\"clicks\"),\n",
    "            F.count(F.col(popularity_target_col)).alias(\"all_impressions\"),\n",
    "        )\n",
    "        .withColumn(\"ctr\", F.col(\"clicks\") / F.col(\"all_impressions\"))\n",
    "    )\n",
    "    lift = (\n",
    "        top_n_ctr.filter(F.col(ranking_above_col) == True)\n",
    "        .select(\"ctr\")\n",
    "        .collect()[0][0]\n",
    "        / top_n_ctr.filter(F.col(ranking_above_col) == False)\n",
    "        .select(\"ctr\")\n",
    "        .collect()[0][0]\n",
    "    )\n",
    "    return lift, top_n_ctr"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "collapsed": true,
     "finishTime": 1782126512120,
     "inputWidgets": {},
     "nuid": "1e825f6a-5c03-4c33-87c8-b550929fd0e2",
     "showTitle": true,
     "startTime": 1782126512026,
     "submitTime": 1782126248753,
     "tableResultSettingsMap": {},
     "title": "ctr at rank"
    }
   },
   "outputs": [],
   "source": [
    "def ctr_at_all_ranks(df):\n",
    "    filtered_df = df.filter(F.col(popularity_target_col).isNotNull())\n",
    "    all_clicks = filtered_df.agg(\n",
    "        F.sum(F.col(popularity_target_col))\n",
    "    ).collect()[0][0]\n",
    "    all_rankdf = (\n",
    "        filtered_df.groupBy(F.col(weighted_ranking_col))\n",
    "        .agg(\n",
    "            F.sum(F.col(popularity_target_col)).alias(\"clicks\"),\n",
    "            F.count(F.col(popularity_target_col)).alias(\"all_impressions\"),\n",
    "        )\n",
    "        .withColumn(\"ctr\", F.col(\"clicks\") / F.col(\"all_impressions\"))\n",
    "        .withColumn(\"perc_total_clicks\", F.col(\"clicks\") / all_clicks)\n",
    "    )\n",
    "    return all_rankdf"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1782126512220,
     "inputWidgets": {},
     "nuid": "3d3491b3-7b78-4afa-9f9a-7d5fe6fd8be7",
     "showTitle": true,
     "startTime": 1782126512125,
     "submitTime": 1782126249496,
     "tableResultSettingsMap": {},
     "title": "Rank 1 distribution"
    }
   },
   "outputs": [],
   "source": [
    "def rank1_advert_distribution(df):\n",
    "    fitered_df = df.filter((F.col(weighted_ranking_col) == 1))\n",
    "    total_number_adverts_rank1 = fitered_df.count()\n",
    "\n",
    "    aggregated_advert_rank1_distribution = (\n",
    "        fitered_df.groupBy(\"uniqueAdID\")\n",
    "        .agg(\n",
    "            F.count(\"uniqueAdID\").alias(\"number_rank1\"),\n",
    "            (F.count(\"uniqueAdID\") / total_number_adverts_rank1).alias(\n",
    "                \"perc_total\"\n",
    "            ),\n",
    "        )\n",
    "        .orderBy(\"number_rank1\")\n",
    "    )\n",
    "    return aggregated_advert_rank1_distribution"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "ddd26a5f-581e-4d71-8f0b-ac934b488679",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## Ranking Evaluation"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1782126574166,
     "inputWidgets": {},
     "nuid": "99c0b62a-ac97-4df7-93ac-0ff5e1495beb",
     "showTitle": true,
     "startTime": 1782126572986,
     "submitTime": 1782126572931,
     "tableResultSettingsMap": {},
     "title": "Build out global advert levels over lookback period"
    }
   },
   "outputs": [],
   "source": [
    "final_validation_scoring = spark.table(final_validation_results_table)\n",
    "clicks_history_table = spark.table(\n",
    "    f\"{catalog_schema_prefix}.{table_prefix}_training_clicks_lookback\"\n",
    ")\n",
    "dates_table = final_validation_scoring.select(\"rundate\").distinct()\n",
    "control_sheet = spark.table(\n",
    "    \"marketingdata_prod.warehouse.next_uk_nextads_control_sheet\"\n",
    ")\n",
    "\n",
    "join_condition = clicks_history_table.date.between(\n",
    "    F.date_sub(dates_table.rundate, lookback_period + 1),\n",
    "    F.date_sub(dates_table.rundate, 1),\n",
    ")\n",
    "\n",
    "overall_ad_impressions = (\n",
    "    clicks_history_table.join(dates_table, on=join_condition, how=\"inner\")\n",
    "    .groupBy(\"rundate\", \"title\", \"campaign\", \"versionnumber\")\n",
    "    .agg(\n",
    "        F.sum(\"number_impressions\").alias(\"num_impressions\"),\n",
    "        F.sum(\"number_clicks\").alias(\"num_clicks\"),\n",
    "    )\n",
    ")\n",
    "\n",
    "overall_impressions = overall_ad_impressions.groupBy(\"rundate\").agg(\n",
    "    F.sum(\"num_impressions\").alias(\"total_num_impressions\"),\n",
    "    F.sum(\"num_clicks\").alias(\"total_num_clicks\"),\n",
    "    (F.sum(\"num_clicks\") / F.sum(\"num_impressions\")).alias(\n",
    "        \"global_clickthrough_rate\"\n",
    "    ),\n",
    "    F.median(\"num_impressions\").alias(\"median_impressions\"),\n",
    ")\n",
    "\n",
    "\n",
    "join_condition_control_sheet = (\n",
    "    (F.upper(control_sheet.CampaignNumber) == overall_ad_impressions.campaign)\n",
    "    & (control_sheet.Title == overall_ad_impressions.title)\n",
    "    & (\n",
    "        overall_ad_impressions.versionnumber\n",
    "        == F.regexp_extract(control_sheet.UniqueAdID, r\"^.*_(V[1-9])_.*$\", 1)\n",
    "    )\n",
    "    & (overall_ad_impressions.rundate == control_sheet.rundate)\n",
    ")\n",
    "\n",
    "global_ads_table = (\n",
    "    overall_ad_impressions.select(\n",
    "        \"rundate\", \"title\", \"campaign\", \"versionnumber\", \"num_impressions\"\n",
    "    )\n",
    "    .join(overall_impressions, on=[\"rundate\"], how=\"inner\")\n",
    "    .join(control_sheet, on=join_condition_control_sheet)\n",
    "    .withColumnsRenamed({\"num_impressions\": \"advert_impressions_30days\"})\n",
    "    .select(\n",
    "        overall_ad_impressions[\"rundate\"],\n",
    "        \"uniqueAdID\",\n",
    "        \"advert_impressions_30days\",\n",
    "        \"median_impressions\",\n",
    "        \"global_clickthrough_rate\",\n",
    "    )\n",
    "    .distinct()\n",
    ")\n",
    "\n",
    "# join_condition =clicks_history_table.date.between(F.date_sub(dates_table.rundate, lookback_period + 1),F.date_sub(dates_table.rundate, 1))\n",
    "\n",
    "# overall_ad_impressions= clicks_history_table.join(dates_table, on=join_condition, how='inner').groupBy('rundate', 'control_sheet_Adid').agg(F.sum('number_impressions').alias('num_impressions'), F.sum('number_clicks').alias('num_clicks'))\n",
    "\n",
    "# overall_impressions= overall_ad_impressions.groupBy(\"rundate\").agg(F.sum('num_impressions').alias('total_num_impressions'), F.sum('num_clicks').alias('total_num_clicks'), (F.sum('num_clicks')/F.sum('num_impressions')).alias('global_clickthrough_rate'), F.median(\"num_impressions\").alias(\"median_impressions\"))\n",
    "\n",
    "# global_ads_table=overall_ad_impressions.select(\"rundate\", \"control_sheet_Adid\", \"num_impressions\").join(overall_impressions, on=[\"rundate\"], how=\"inner\").withColumnsRenamed({\"control_sheet_Adid\": \"uniqueAdID\", \"num_impressions\": \"advert_impressions_30days\"}\n",
    "# ).select(\"rundate\", \"uniqueAdID\", \"advert_impressions_30days\", \"median_impressions\", \"global_clickthrough_rate\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1782126580370,
     "inputWidgets": {},
     "nuid": "37eaac40-4e4f-4508-a2c0-ccd221a5f5c5",
     "showTitle": true,
     "startTime": 1782126578560,
     "submitTime": 1782126578528,
     "tableResultSettingsMap": {},
     "title": "Ranking multiplication factor"
    }
   },
   "outputs": [],
   "source": [
    "## Determine the scoring multiplication factor:\n",
    "affinity_multiplication_factor = (\n",
    "    final_validation_scoring.groupBy()\n",
    "    .agg(\n",
    "        F.stddev(F.col(regressor_predictions_col)).alias(\"regressor_stddev\"),\n",
    "        F.stddev(F.col(popularity_click_prob_col)).alias(\"popularity_stddev\"),\n",
    "    )\n",
    "    .withColumn(\n",
    "        \"multiplication_factor\",\n",
    "        F.round(F.col(\"popularity_stddev\") / F.col(\"regressor_stddev\"), 1),\n",
    "    )\n",
    "    .select(\"multiplication_factor\")\n",
    "    .collect()[0][0]\n",
    ")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1782126582072,
     "inputWidgets": {},
     "nuid": "e0d5dd18-dec3-4e78-8de0-2de40a6363f5",
     "showTitle": false,
     "startTime": 1782126581910,
     "submitTime": 1782126581886,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "display(affinity_multiplication_factor)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1782126855627,
     "inputWidgets": {},
     "nuid": "40c13bea-146c-4a6c-b38b-c201f14d01b9",
     "showTitle": false,
     "startTime": 1782126855536,
     "submitTime": 1782126855508,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "## Based on the above as guidance set the affinity weighting\n",
    "affinity_weighting_factor = 4"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1782126587476,
     "inputWidgets": {},
     "nuid": "6ea88ca6-292e-4c24-b5fd-b1ae3681a3d4",
     "showTitle": true,
     "startTime": 1782126587108,
     "submitTime": 1782126587084,
     "tableResultSettingsMap": {},
     "title": "Weighted Ranking"
    }
   },
   "outputs": [],
   "source": [
    "best_pred_val = (\n",
    "    final_validation_scoring.join(\n",
    "        global_ads_table, how=\"left\", on=[\"rundate\", \"uniqueAdID\"]\n",
    "    )\n",
    "    .withColumn(\n",
    "        \"popularity_scoring_multiplier\",\n",
    "        (\n",
    "            (F.col(\"advert_impressions_30days\") + 1)\n",
    "            / (\n",
    "                F.col(\"advert_impressions_30days\")\n",
    "                + 1\n",
    "                + F.col(\"median_impressions\")\n",
    "            )\n",
    "        ),\n",
    "    )\n",
    "    .withColumn(\n",
    "        popularity_smoothed_score_col,\n",
    "        F.col(popularity_click_prob_col)\n",
    "        * F.col(\"popularity_scoring_multiplier\"),\n",
    "    )\n",
    "    .withColumn(\n",
    "        regression_weighted_score_col,\n",
    "        F.col(regressor_predictions_col) * affinity_weighting_factor,\n",
    "    )\n",
    "    .withColumn(\n",
    "        combined_score_col,\n",
    "        F.col(regression_weighted_score_col)\n",
    "        + F.col(popularity_smoothed_score_col),\n",
    "    )\n",
    "    .withColumn(\n",
    "        weighted_ranking_col,\n",
    "        F.dense_rank().over(\n",
    "            Window.partitionBy(\"rundate\", \"account_number\").orderBy(\n",
    "                F.desc(combined_score_col)\n",
    "            )\n",
    "        ),\n",
    "    )\n",
    ")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1782126643975,
     "inputWidgets": {},
     "nuid": "8d5ec535-4913-428a-a7e2-6eb69d9149ec",
     "showTitle": true,
     "startTime": 1782126589814,
     "submitTime": 1782126589787,
     "tableResultSettingsMap": {},
     "title": "Validation ranking metrics"
    }
   },
   "outputs": [],
   "source": [
    "lift_2above, rank_2above_ctr = rank_n_and_above_lift_metrics(best_pred_val, 2)\n",
    "all_rank_ctrs = ctr_at_all_ranks(best_pred_val)\n",
    "advert_rank1_distribution = rank1_advert_distribution(best_pred_val)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1782126644173,
     "inputWidgets": {},
     "nuid": "a9e9aa31-2159-4884-ade9-ae90f1182c04",
     "showTitle": false,
     "startTime": 1782126644013,
     "submitTime": 1782126596865,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "lift_2above"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1782126659488,
     "inputWidgets": {},
     "nuid": "2bb1e242-31bc-4d4b-ba61-d6de5d5a744f",
     "showTitle": false,
     "startTime": 1782126644181,
     "submitTime": 1782126603018,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "display(rank_2above_ctr)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1782126678009,
     "inputWidgets": {},
     "nuid": "ce7cd8e5-86c3-48b6-a88e-2e372e4195b3",
     "showTitle": false,
     "startTime": 1782126659499,
     "submitTime": 1782126639038,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "display(all_rank_ctrs.orderBy(F.col(weighted_ranking_col)))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1782126694823,
     "inputWidgets": {},
     "nuid": "f8bba053-f415-4b49-bc42-f873a0aa1ef7",
     "showTitle": false,
     "startTime": 1782126678017,
     "submitTime": 1782126643751,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "display(advert_rank1_distribution.orderBy(F.desc(\"perc_total\")))"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "c7859b79-f7df-4db6-b238-2f770c60aeaa",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "## Register Best Models\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1782126873740,
     "inputWidgets": {},
     "nuid": "990587a2-934a-419c-952f-24e0fd976338",
     "showTitle": false,
     "startTime": 1782126862886,
     "submitTime": 1782126862858,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "classifier_model = f\"runs:/{classifier_model_run_id}/model\"\n",
    "regressor_model = f\"runs:/{regressor_model_run_id}/model\"\n",
    "\n",
    "\n",
    "classifier_registered_model = mlflow.register_model(\n",
    "    model_uri=classifier_model,\n",
    "    name=classifier_registered_model_name,\n",
    ")\n",
    "\n",
    "regressor_registered_model = mlflow.register_model(\n",
    "    model_uri=regressor_model,\n",
    "    name=regresssor_registered_model_name,\n",
    ")"
   ]
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "computePreferences": null,
   "dashboards": [],
   "environmentMetadata": {
    "base_environment": "",
    "environment_version": "5"
   },
   "inputWidgetPreferences": null,
   "language": "python",
   "notebookMetadata": {
    "experimentId": "589860896018190",
    "mostRecentlyExecutedCommandWithImplicitDF": {
     "commandId": 8917929716887965,
     "dataframes": [
      "_sqldf"
     ]
    },
    "pythonIndentUnit": 4
   },
   "notebookName": "train_model.py",
   "widgets": {
    "catalog_schema_prefix": {
     "currentValue": "marketingdata_dev.claire_wilsonbarnes",
     "nuid": "9cc1635e-6369-4f69-8f95-62b2fd6d6c14",
     "typedWidgetInfo": {
      "autoCreated": false,
      "defaultValue": "marketingdata_dev.claire_wilsonbarnes",
      "dynamic": false,
      "label": null,
      "name": "catalog_schema_prefix",
      "options": {
       "validationRegex": null,
       "widgetDisplayType": "Text"
      },
      "parameterDataType": "String"
     },
     "widgetInfo": {
      "defaultValue": "marketingdata_dev.claire_wilsonbarnes",
      "label": null,
      "name": "catalog_schema_prefix",
      "options": {
       "autoCreated": null,
       "validationRegex": null,
       "widgetType": "text"
      },
      "widgetType": "text"
     }
    },
    "lookback_period": {
     "currentValue": "30",
     "nuid": "53163900-f221-47ee-8879-5a8912213984",
     "typedWidgetInfo": {
      "autoCreated": false,
      "defaultValue": "30",
      "dynamic": false,
      "label": null,
      "name": "lookback_period",
      "options": {
       "validationRegex": null,
       "widgetDisplayType": "Text"
      },
      "parameterDataType": "String"
     },
     "widgetInfo": {
      "defaultValue": "30",
      "label": null,
      "name": "lookback_period",
      "options": {
       "autoCreated": null,
       "validationRegex": null,
       "widgetType": "text"
      },
      "widgetType": "text"
     }
    },
    "table_prefix": {
     "currentValue": "next_uk_nextAds_analytics_pctr",
     "nuid": "4cfd1540-150a-4bb1-ad69-e102b283eefa",
     "typedWidgetInfo": {
      "autoCreated": false,
      "defaultValue": "next_uk_nextAds_analytics_pctr",
      "dynamic": false,
      "label": null,
      "name": "table_prefix",
      "options": {
       "validationRegex": null,
       "widgetDisplayType": "Text"
      },
      "parameterDataType": "String"
     },
     "widgetInfo": {
      "defaultValue": "next_uk_nextAds_analytics_pctr",
      "label": null,
      "name": "table_prefix",
      "options": {
       "autoCreated": false,
       "validationRegex": null,
       "widgetType": "text"
      },
      "widgetType": "text"
     }
    }
   }
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
