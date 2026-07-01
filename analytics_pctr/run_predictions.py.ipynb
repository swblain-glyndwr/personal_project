{
 "cells": [
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
     "finishTime": 1782894249058,
     "inputWidgets": {},
     "nuid": "082d2004-949f-45b1-8b96-b8059323786b",
     "showTitle": false,
     "startTime": 1782894230683,
     "submitTime": 1782894210939,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "#!pip install \"/Workspace/Users/claire_wilsonbarnes@next.co.uk/next-ads/wheels/dsutils-0.1.13-py3-none-any.whl\""
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
     "finishTime": 1782894249440,
     "inputWidgets": {},
     "nuid": "e9a20342-6e1f-428c-b9d8-6a09e208a43e",
     "showTitle": false,
     "startTime": 1782894249113,
     "submitTime": 1782894211642,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "import mlflow\n",
    "import mlflow.spark\n",
    "\n",
    "from pyspark.sql import functions as F\n",
    "from pyspark.sql import Window\n",
    "\n",
    "# Pipelining\n",
    "from pyspark.ml.functions import vector_to_array\n",
    "\n",
    "# Models\n",
    "from dsutils.etl import (\n",
    "    truncate_and_load,\n",
    "    delete_from_and_load,\n",
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
     "finishTime": 1782894249639,
     "inputWidgets": {},
     "nuid": "f0e2e7af-95a2-4804-833f-757dc86b822b",
     "showTitle": false,
     "startTime": 1782894249464,
     "submitTime": 1782894213898,
     "tableResultSettingsMap": {},
     "title": ""
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
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1782894249740,
     "inputWidgets": {},
     "nuid": "a8b0d4e4-a9a4-435c-85cf-35cfe99bf9fa",
     "showTitle": true,
     "startTime": 1782894249654,
     "submitTime": 1782894214361,
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
     "finishTime": 1782894249941,
     "inputWidgets": {},
     "nuid": "369e9b5a-2a3f-43bc-9f18-c2cdebcffb69",
     "showTitle": true,
     "startTime": 1782894249757,
     "submitTime": 1782894215678,
     "tableResultSettingsMap": {},
     "title": "Widgets"
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
    "    name=\"lookback_period\", defaultValue=\"30\", label=\"lookback_period\"\n",
    ")\n",
    "dbutils.widgets.text(\n",
    "    name=\"table_prefix\",\n",
    "    defaultValue=\"next_uk_nextAds_analytics_pctr\",\n",
    "    label=\"table_prefix\",\n",
    ")\n",
    "dbutils.widgets.text(\n",
    "    name=\"affinity_weighting_factor\",\n",
    "    defaultValue=\"4\",\n",
    "    label=\"affinity_weighting_factor\",\n",
    ")\n",
    "dbutils.widgets.text(\n",
    "    name=\"regressor_model_uri\",\n",
    "    defaultValue=\"models:/marketingdata_dev.ds_sandbox.nextads_analytics_pctr_affinity_regression_model/2\",\n",
    "    label=\"regressor_model_uri\",\n",
    ")\n",
    "dbutils.widgets.text(\n",
    "    name=\"classifier_model_uri\",\n",
    "    defaultValue=\"models:/marketingdata_dev.ds_sandbox.nextads_analytics_pctr_popularity_classification_model/2\",\n",
    "    label=\"classifier_model_uri\",\n",
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
     "finishTime": 1782904098514,
     "inputWidgets": {},
     "nuid": "503affd6-b7b6-420c-b6f1-ce1d607bfd76",
     "showTitle": true,
     "startTime": 1782904098422,
     "submitTime": 1782903972446,
     "tableResultSettingsMap": {},
     "title": "Variables"
    }
   },
   "outputs": [],
   "source": [
    "catalog_schema_prefix = get_widget_value(\n",
    "    \"catalog_schema_prefix\", \"marketingdata_dev.claire_wilsonbarnes\"\n",
    ")\n",
    "lookback_period = int(get_widget_value(\"lookback_period\", \"30\"))\n",
    "table_prefix = get_widget_value(\n",
    "    \"table_prefix\", \"next_uk_nextAds_analytics_pctr\"\n",
    ")\n",
    "affinity_weighting_factor = int(\n",
    "    get_widget_value(\"affinity_weighting_factor\", \"4\")\n",
    ")\n",
    "regressor_model_uri = get_widget_value(\n",
    "    \"regressor_model_uri\",\n",
    "    \"models:/marketingdata_dev.ds_sandbox.nextads_analytics_pctr_affinity_regression_model/2\",\n",
    ")\n",
    "classifier_model_uri = get_widget_value(\n",
    "    \"classifier_model_uri\",\n",
    "    \"models:/marketingdata_dev.ds_sandbox.nextads_analytics_pctr_popularity_classification_model/2\",\n",
    ")\n",
    "\n",
    "FEATURE_TABLE = catalog_schema_prefix + \".\" + table_prefix + \"_features\"\n",
    "TARGET_TABLE = catalog_schema_prefix + \".\" + table_prefix + \"_predictions\"\n",
    "TARGET_TABLE_LATEST = (\n",
    "    catalog_schema_prefix + \".\" + table_prefix + \"_predictions_latest\"\n",
    ")\n",
    "\n",
    "fill_zeros_columns = {\n",
    "    \"day_impressions\": 0,\n",
    "    \"prior_day_impressions\": 0,\n",
    "    \"week_impressions\": 0,\n",
    "    \"prior_week_impressions\": 0,\n",
    "    \"customer_total_clicks\": 0,\n",
    "    \"customer_total_unique_adverts_clicked\": 0,\n",
    "    \"customer_advert_previous_click_number\": 0,\n",
    "    \"number_clicks_same_algodivision\": 0,\n",
    "    \"view_highest_catid_weight\": 0,\n",
    "    \"view_lift_adjusted\": 0,\n",
    "    \"view_cs\": 0,\n",
    "    \"purchase_highest_catid_weight\": 0,\n",
    "    \"purchase_lift_adjusted\": 0,\n",
    "    \"purchase_cs\": 0,\n",
    "}\n",
    "\n",
    "popularity_smoothed_score_col = \"popularity_smoothed_score\"\n",
    "regression_weighted_score_col = \"regression_weighted_score\"\n",
    "popularity_click_prob_col = \"popularity_prob_click\"\n",
    "popularity_probability_col = \"probability\"\n",
    "regressor_predictions_col = \"residual_predictions\"\n",
    "combined_score_col = \"combined_weighted_score\"\n",
    "weighted_ranking_col = \"weighted_ranking\"\n",
    "pk_cols = [\"account_number\", \"UniqueAdID\"]\n",
    "\n",
    "target_cols = pk_cols + [\n",
    "    popularity_smoothed_score_col,\n",
    "    regression_weighted_score_col,\n",
    "    popularity_click_prob_col,\n",
    "    regressor_predictions_col,\n",
    "    combined_score_col,\n",
    "    weighted_ranking_col,\n",
    "    \"advert_impressions_30days\",\n",
    "    \"advert_item_revenue\",\n",
    "    #'rundate',\n",
    "]\n",
    "\n",
    "# QA Thresholds\n",
    "distribution_number_ads_threshold = 15\n",
    "cumulative_coverage_threshold = 0.8\n",
    "rank1_advert_coverage_threshold = 0.25"
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
     "finishTime": 1782894261989,
     "inputWidgets": {},
     "nuid": "fea51de1-5deb-4bc3-8d0d-6b720697b97c",
     "showTitle": true,
     "startTime": 1782894250169,
     "submitTime": 1782894220481,
     "tableResultSettingsMap": {},
     "title": "Data Sources"
    }
   },
   "outputs": [],
   "source": [
    "clicks_history_table = spark.table(\n",
    "    catalog_schema_prefix + \".\" + table_prefix + \"_training_clicks_lookback\"\n",
    ")\n",
    "current_control_sheet = spark.table(\n",
    "    \"marketingdata_prod.warehouse.next_uk_nextads_control_sheet_latest\"\n",
    ")\n",
    "\n",
    "ad_items_table = spark.table(\n",
    "    \"marketingdata_prod.warehouse.next_ads_sort_order_latest\"\n",
    ").select(\"uniqueAdID\", \"items\")\n",
    "baskets_table = spark.table(\n",
    "    \"marketingdata_prod.warehouse.baskets_uk_3y\"\n",
    ").filter(F.col(\"order_date\") >= F.date_sub(F.current_date(), lookback_period))"
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
     "finishTime": 1782894262657,
     "inputWidgets": {},
     "nuid": "ead5d2d0-a6d5-40f7-b76f-254b59c2ae59",
     "showTitle": true,
     "startTime": 1782894262018,
     "submitTime": 1782894221996,
     "tableResultSettingsMap": {},
     "title": "Input table"
    }
   },
   "outputs": [],
   "source": [
    "# pctr_prediction_features\n",
    "predictions_input = spark.table(FEATURE_TABLE)\n",
    "predictions_input = predictions_input.fillna(fill_zeros_columns)"
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
     "finishTime": 1782894301385,
     "inputWidgets": {},
     "nuid": "9b42314b-2db7-43c3-a11e-8eaf55c9b6b5",
     "showTitle": true,
     "startTime": 1782894262675,
     "submitTime": 1782894224397,
     "tableResultSettingsMap": {},
     "title": "Load Models"
    }
   },
   "outputs": [],
   "source": [
    "mlflow.set_registry_uri(\"databricks-uc\")\n",
    "\n",
    "try:\n",
    "    popularity_model = mlflow.spark.load_model(classifier_model_uri)\n",
    "    affinity_model = mlflow.spark.load_model(regressor_model_uri)\n",
    "\n",
    "except Exception as e:\n",
    "    print(f\"Error in loading models :{e}\")"
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
     "finishTime": 1782894307808,
     "inputWidgets": {},
     "nuid": "e988ffdf-a164-4a32-b10b-2ac104050223",
     "showTitle": true,
     "startTime": 1782894301408,
     "submitTime": 1782894225565,
     "tableResultSettingsMap": {},
     "title": "Predictions"
    }
   },
   "outputs": [],
   "source": [
    "popularity_scored_df = popularity_model.transform(predictions_input)\n",
    "affinity_scored_df = affinity_model.transform(popularity_scored_df)"
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
     "finishTime": 1782894308509,
     "inputWidgets": {},
     "nuid": "93b51edf-a165-4eb5-a3b9-77af095469b6",
     "showTitle": true,
     "startTime": 1782894307824,
     "submitTime": 1782894227516,
     "tableResultSettingsMap": {},
     "title": "Adverts Click data- to utilize for smoothing of score"
    }
   },
   "outputs": [],
   "source": [
    "# Addition of advert click data over the last 30 days\n",
    "dates_table = affinity_scored_df.select(\"rundate\").distinct()\n",
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
     "finishTime": 1782894309311,
     "inputWidgets": {},
     "nuid": "737c6d0b-55e7-47cb-bf7b-a5c1387e40a4",
     "showTitle": true,
     "startTime": 1782894308524,
     "submitTime": 1782894231769,
     "tableResultSettingsMap": {},
     "title": "Addition of control sheet to get uniqueAdvertIDs"
    }
   },
   "outputs": [],
   "source": [
    "join_condition_control_sheet = (\n",
    "    (\n",
    "        F.upper(current_control_sheet.CampaignNumber)\n",
    "        == clicks_history_table.campaign\n",
    "    )\n",
    "    & (current_control_sheet.Title == clicks_history_table.title)\n",
    "    & (\n",
    "        clicks_history_table.versionnumber\n",
    "        == F.regexp_extract(\n",
    "            current_control_sheet.UniqueAdID, r\"^.*_(V[1-9])_.*$\", 1\n",
    "        )\n",
    "    )\n",
    ")\n",
    "\n",
    "global_ads_table = (\n",
    "    overall_ad_impressions.select(\n",
    "        \"rundate\", \"title\", \"campaign\", \"versionnumber\", \"num_impressions\"\n",
    "    )\n",
    "    .join(overall_impressions, on=[\"rundate\"], how=\"inner\")\n",
    "    .join(current_control_sheet, on=join_condition_control_sheet)\n",
    "    .withColumnsRenamed({\"num_impressions\": \"advert_impressions_30days\"})\n",
    "    .select(\n",
    "        overall_ad_impressions[\"rundate\"],\n",
    "        \"uniqueAdID\",\n",
    "        \"advert_impressions_30days\",\n",
    "        \"median_impressions\",\n",
    "        \"global_clickthrough_rate\",\n",
    "    )\n",
    "    .distinct()\n",
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
     "finishTime": 1782899938095,
     "inputWidgets": {},
     "nuid": "6a4f5330-0e25-4d0b-8141-4dadc9f1be6e",
     "showTitle": true,
     "startTime": 1782899923370,
     "submitTime": 1782899923321,
     "tableResultSettingsMap": {},
     "title": "Median Impression number"
    }
   },
   "outputs": [],
   "source": [
    "median_impressions=global_ads_table.filter(F.col(\"median_impressions\").isNotNull()).dropDuplicates([\"median_impressions\"]).select(\"median_impressions\").collect()[0][0]"
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
     "finishTime": 1782894394625,
     "inputWidgets": {},
     "nuid": "9bbb61d9-1074-4298-bcaf-880d5ec90734",
     "showTitle": true,
     "startTime": 1782894394432,
     "submitTime": 1782894394397,
     "tableResultSettingsMap": {},
     "title": "Advert Item Revenue"
    }
   },
   "outputs": [],
   "source": [
    "## Addition of items from adverts revenue as a tiebreaker if necessary\n",
    "\n",
    "ads_item_revenue_last_30days = (\n",
    "    ad_items_table.join(\n",
    "        baskets_table,\n",
    "        how=\"left\",\n",
    "        on=[baskets_table[\"itemno\"] == ad_items_table[\"items\"]],\n",
    "    )\n",
    "    .groupBy(\"uniqueAdID\")\n",
    "    .agg(F.sum(F.col(\"s740orderstakenvalue\")).alias(\"advert_item_revenue\"))\n",
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
     "finishTime": 1782900158716,
     "inputWidgets": {},
     "nuid": "ed1ed0eb-8512-4d0c-af83-66fa1e7b345e",
     "showTitle": true,
     "startTime": 1782900158185,
     "submitTime": 1782900158123,
     "tableResultSettingsMap": {},
     "title": "Ranking of Adverts"
    }
   },
   "outputs": [],
   "source": [
    "combined_data=affinity_scored_df.join(\n",
    "        global_ads_table, how=\"left\", on=[\"rundate\", \"uniqueAdID\"]\n",
    "    ).join(ads_item_revenue_last_30days, how=\"left\", on=[\"uniqueAdID\"]).withColumn(\"advert_impressions_30days\", F.coalesce(F.col(\"advert_impressions_30days\"), F.lit(0))).withColumn(\"advert_item_revenue\", F.coalesce(F.col(\"advert_item_revenue\"),F.lit(0))).withColumn(\"median_impressions\", F.lit(median_impressions))\n",
    "\n",
    "predictions = (combined_data.withColumn(\n",
    "        \"popularity_scoring_multiplier\",\n",
    "        F.coalesce(\n",
    "            (\n",
    "                (F.col(\"advert_impressions_30days\") + 1)\n",
    "                / (\n",
    "                    F.col(\"advert_impressions_30days\")\n",
    "                    + 1\n",
    "                    + F.col(\"median_impressions\")\n",
    "                )\n",
    "            ),\n",
    "            F.lit(0),\n",
    "        ),\n",
    "    ).withColumn(\n",
    "        popularity_click_prob_col,\n",
    "        vector_to_array(F.col(popularity_probability_col))[1],\n",
    "    ).withColumn(\n",
    "        popularity_smoothed_score_col,\n",
    "        F.col(popularity_click_prob_col)\n",
    "        * F.col(\"popularity_scoring_multiplier\"),\n",
    "    ).withColumn(\n",
    "        regression_weighted_score_col,\n",
    "        F.col(regressor_predictions_col) * affinity_weighting_factor,\n",
    "    ).withColumn(\n",
    "        combined_score_col,\n",
    "        F.col(regression_weighted_score_col)\n",
    "        + F.col(popularity_smoothed_score_col),\n",
    "    ).withColumn(\n",
    "        weighted_ranking_col,\n",
    "        F.dense_rank().over(\n",
    "            Window.partitionBy(\"rundate\", \"account_number\").orderBy(\n",
    "                F.desc(combined_score_col),\n",
    "                F.desc(\"advert_impressions_30days\"),\n",
    "                F.desc(\"advert_item_revenue\"),\n",
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
     "finishTime": 1782901569644,
     "inputWidgets": {},
     "nuid": "bb929bb4-78a4-4e92-862a-e771dfd07512",
     "showTitle": true,
     "startTime": 1782900168443,
     "submitTime": 1782900168388,
     "tableResultSettingsMap": {},
     "title": "Write output to table "
    }
   },
   "outputs": [],
   "source": [
    "print(\"Loading output to table (latest)\")\n",
    "truncate_and_load(\n",
    "    predictions.select(*target_cols),\n",
    "    TARGET_TABLE_LATEST,\n",
    "    pk_cols=pk_cols,\n",
    ")\n",
    "# predictions.select(*target_cols).write.mode(\"overwrite\").option(\"overwriteSchema\", \"true\").format(\"delta\").saveAsTable(TARGET_TABLE_LATEST)"
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
     "finishTime": 1782142327063,
     "inputWidgets": {},
     "nuid": "284c6897-2dae-4f2f-b8bf-3be4f85d62fd",
     "showTitle": true,
     "startTime": 1782141276904,
     "submitTime": 1782141276878,
     "tableResultSettingsMap": {},
     "title": "Create a history log of records"
    }
   },
   "outputs": [],
   "source": [
    "print(\"Loading output to table\")\n",
    "delete_from_and_load(\n",
    "    predictions.select(*target_cols),\n",
    "    TARGET_TABLE,\n",
    "    pk_cols=pk_cols,\n",
    "    del_where={\"rundate\": \"current_date()\"},\n",
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
     "finishTime": 1782903943679,
     "inputWidgets": {},
     "nuid": "fac1d132-606b-43de-b432-44c24ef13344",
     "showTitle": true,
     "startTime": 1782903943263,
     "submitTime": 1782903943186,
     "tableResultSettingsMap": {},
     "title": "Validate the predictions"
    }
   },
   "outputs": [],
   "source": [
    "prediction_scores = spark.table(TARGET_TABLE_LATEST)"
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
     "finishTime": 1782904098414,
     "inputWidgets": {},
     "nuid": "945fbbef-f66d-438e-86d8-d285a2c232f5",
     "showTitle": true,
     "startTime": 1782903944622,
     "submitTime": 1782903944584,
     "tableResultSettingsMap": {},
     "title": "QA tests"
    }
   },
   "outputs": [],
   "source": [
    "## Run QA\n",
    "errors = []\n",
    "\n",
    "## Only 1 run date and is current date\n",
    "distinct_rundates = prediction_scores.select(\"rundate\").distinct().count()\n",
    "try:\n",
    "    assert distinct_rundates == 1, (\n",
    "        f\"Multiple rundates in {TARGET_TABLE_LATEST}\"\n",
    "    )\n",
    "except AssertionError as e:\n",
    "    errors.append(str(e))\n",
    "\n",
    "# rank 1 & 2 have as many predictions as input customers\n",
    "rank_1_number_predictions = prediction_scores.filter(\n",
    "    F.col(weighted_ranking_col) == 1\n",
    ").count()\n",
    "rank_2_number_predictions = prediction_scores.filter(\n",
    "    F.col(weighted_ranking_col) == 2\n",
    ").count()\n",
    "number_customers = (\n",
    "    predictions_input.select(\"account_number\").distinct().count()\n",
    ")\n",
    "\n",
    "try:\n",
    "    assert rank_1_number_predictions == number_customers, (\n",
    "        f\"Number of rank 1 predictions {rank_1_number_predictions} does not match number of customers {number_customers}\"\n",
    "    )\n",
    "except AssertionError as e:\n",
    "    errors.append(str(e))\n",
    "\n",
    "try:\n",
    "    assert rank_2_number_predictions == number_customers, (\n",
    "        f\"Number of rank 2 predictions {rank_2_number_predictions} does not match number of customers {number_customers}\"\n",
    "    )\n",
    "except AssertionError as e:\n",
    "    errors.append(str(e))\n",
    "\n",
    "# number of adverts covered in rank 1 & 2\n",
    "filtered_df = prediction_scores.filter(F.col(weighted_ranking_col).isin(1, 2))\n",
    "total_prediction_number = filtered_df.count()\n",
    "total_number_adverts_rank12 = (\n",
    "    filtered_df.select(\"uniqueAdID\").distinct().count()\n",
    ")\n",
    "# At least 50% of adverts available represented in rank 1 & 2 positions\n",
    "min_threshold_number_of_ads = round(\n",
    "    predictions_input.select(\"uniqueAdID\").distinct().count() / 2, 0\n",
    ")\n",
    "\n",
    "try:\n",
    "    assert total_number_adverts_rank12 >= min_threshold_number_of_ads, (\n",
    "        \"Less than 50% of adverts available represented in rank 1 & 2 positions\"\n",
    "    )\n",
    "except AssertionError as e:\n",
    "    errors.append(str(e))\n",
    "\n",
    "## Advert distribution\n",
    "cumulative_sum_advert_coverage_window = Window.orderBy(\n",
    "    F.desc(\"perc_total\")\n",
    ").rowsBetween(Window.unboundedPreceding, Window.currentRow)\n",
    "aggregated_advert_rank1_distribution = (\n",
    "    filtered_df.groupBy(\"uniqueAdID\")\n",
    "    .agg(\n",
    "        F.count(\"uniqueAdID\").alias(\"rank1_2\"),\n",
    "        (F.count(\"uniqueAdID\") / total_prediction_number).alias(\"perc_total\"),\n",
    "    )\n",
    "    .orderBy(F.desc(\"perc_total\"))\n",
    "    .withColumn(\n",
    "        \"cumulative_coverage\",\n",
    "        F.sum(\"perc_total\").over(cumulative_sum_advert_coverage_window),\n",
    "    )\n",
    ")\n",
    "\n",
    "# top 80% distribution\n",
    "number_ads_cumulative_coverage = aggregated_advert_rank1_distribution.filter(\n",
    "    F.col(\"cumulative_coverage\") <= cumulative_coverage_threshold\n",
    ").count()\n",
    "try:\n",
    "    assert (\n",
    "        number_ads_cumulative_coverage >= distribution_number_ads_threshold\n",
    "    ), (\n",
    "        f\"Less than {distribution_number_ads_threshold} ads cover {cumulative_coverage_threshold * 100}% of all rank 1 & 2 positions\"\n",
    "    )\n",
    "except AssertionError as e:\n",
    "    errors.append(str(e))\n",
    "\n",
    "\n",
    "# max coverage percentage - does this meet the threshold\n",
    "\n",
    "rank1_coverage_perc = aggregated_advert_rank1_distribution.select(\n",
    "    \"perc_total\"\n",
    ").collect()[0][0]\n",
    "try:\n",
    "    assert rank1_coverage_perc <= rank1_advert_coverage_threshold, (\n",
    "        \"Top ranked advert covers 25% or more of all rank 1 & 2 positions\"\n",
    "    )\n",
    "except AssertionError as e:\n",
    "    errors.append(str(e))"
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
     "finishTime": 1782904101618,
     "inputWidgets": {},
     "nuid": "060af298-5ce1-4e97-8875-8301ead253c2",
     "showTitle": true,
     "startTime": 1782904101484,
     "submitTime": 1782904101403,
     "tableResultSettingsMap": {},
     "title": "Raise QA errors"
    }
   },
   "outputs": [],
   "source": [
    "if errors:\n",
    "    final_errors = \"\\n\".join(errors)\n",
    "    print(final_errors)\n",
    "    raise AssertionError(final_errors)"
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
    "mostRecentlyExecutedCommandWithImplicitDF": {
     "commandId": 6283423869708432,
     "dataframes": [
      "_sqldf"
     ]
    },
    "pythonIndentUnit": 4
   },
   "notebookName": "run_predictions.py",
   "widgets": {
    "affinity_weighting_factor": {
     "currentValue": "4",
     "nuid": "5089dfc5-e921-44ee-b0a5-741965e07d7d",
     "typedWidgetInfo": {
      "autoCreated": false,
      "defaultValue": "4",
      "label": null,
      "name": "affinity_weighting_factor",
      "options": {
       "widgetDisplayType": "Text",
       "validationRegex": null
      },
      "parameterDataType": "String",
      "dynamic": false
     },
     "widgetInfo": {
      "widgetType": "text",
      "defaultValue": "4",
      "label": null,
      "name": "affinity_weighting_factor",
      "options": {
       "widgetType": "text",
       "autoCreated": null,
       "validationRegex": null
      }
     }
    },
    "catalog_schema_prefix": {
     "currentValue": "marketingdata_dev.claire_wilsonbarnes",
     "nuid": "2ce7334c-10e2-4af2-9871-bfb15b7ce8b9",
     "typedWidgetInfo": {
      "autoCreated": false,
      "defaultValue": "marketingdata_dev.claire_wilsonbarnes",
      "label": null,
      "name": "catalog_schema_prefix",
      "options": {
       "widgetDisplayType": "Text",
       "validationRegex": null
      },
      "parameterDataType": "String",
      "dynamic": false
     },
     "widgetInfo": {
      "widgetType": "text",
      "defaultValue": "marketingdata_dev.claire_wilsonbarnes",
      "label": null,
      "name": "catalog_schema_prefix",
      "options": {
       "widgetType": "text",
       "autoCreated": null,
       "validationRegex": null
      }
     }
    },
    "classifier_model_uri": {
     "currentValue": "models:/marketingdata_dev.ds_sandbox.nextads_analytics_pctr_popularity_classification_model/2",
     "nuid": "0c7e2090-904f-4ac7-aa96-8a45e4c49506",
     "typedWidgetInfo": {
      "autoCreated": false,
      "defaultValue": "models:/marketingdata_dev.ds_sandbox.nextads_analytics_pctr_popularity_classification_model/2",
      "label": null,
      "name": "classifier_model_uri",
      "options": {
       "widgetDisplayType": "Text",
       "validationRegex": null
      },
      "parameterDataType": "String",
      "dynamic": false
     },
     "widgetInfo": {
      "widgetType": "text",
      "defaultValue": "models:/marketingdata_dev.ds_sandbox.nextads_analytics_pctr_popularity_classification_model/2",
      "label": null,
      "name": "classifier_model_uri",
      "options": {
       "widgetType": "text",
       "autoCreated": null,
       "validationRegex": null
      }
     }
    },
    "lookback_period": {
     "currentValue": "30",
     "nuid": "2bd5b38a-4b78-4263-9d2c-ab3a357a11b0",
     "typedWidgetInfo": {
      "autoCreated": false,
      "defaultValue": "30",
      "label": null,
      "name": "lookback_period",
      "options": {
       "widgetDisplayType": "Text",
       "validationRegex": null
      },
      "parameterDataType": "String",
      "dynamic": false
     },
     "widgetInfo": {
      "widgetType": "text",
      "defaultValue": "30",
      "label": null,
      "name": "lookback_period",
      "options": {
       "widgetType": "text",
       "autoCreated": null,
       "validationRegex": null
      }
     }
    },
    "regressor_model_uri": {
     "currentValue": "models:/marketingdata_dev.ds_sandbox.nextads_analytics_pctr_affinity_regression_model/2",
     "nuid": "d2292311-1add-4b11-b8ea-e50518139024",
     "typedWidgetInfo": {
      "autoCreated": false,
      "defaultValue": "models:/marketingdata_dev.ds_sandbox.nextads_analytics_pctr_affinity_regression_model/2",
      "label": null,
      "name": "regressor_model_uri",
      "options": {
       "widgetDisplayType": "Text",
       "validationRegex": null
      },
      "parameterDataType": "String",
      "dynamic": false
     },
     "widgetInfo": {
      "widgetType": "text",
      "defaultValue": "models:/marketingdata_dev.ds_sandbox.nextads_analytics_pctr_affinity_regression_model/2",
      "label": null,
      "name": "regressor_model_uri",
      "options": {
       "widgetType": "text",
       "autoCreated": null,
       "validationRegex": null
      }
     }
    },
    "table_prefix": {
     "currentValue": "next_uk_nextAds_analytics_pctr",
     "nuid": "10026e7a-3711-4226-8b4e-9abde9d94edf",
     "typedWidgetInfo": {
      "autoCreated": false,
      "defaultValue": "next_uk_nextAds_analytics_pctr",
      "label": null,
      "name": "table_prefix",
      "options": {
       "widgetDisplayType": "Text",
       "validationRegex": null
      },
      "parameterDataType": "String",
      "dynamic": false
     },
     "widgetInfo": {
      "widgetType": "text",
      "defaultValue": "next_uk_nextAds_analytics_pctr",
      "label": null,
      "name": "table_prefix",
      "options": {
       "widgetType": "text",
       "autoCreated": null,
       "validationRegex": null
      }
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
