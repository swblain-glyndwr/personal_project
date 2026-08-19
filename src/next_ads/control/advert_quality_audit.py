from datetime import datetime, timedelta

from pyspark.sql import DataFrame, Window, SparkSession
from pyspark.sql import functions as F
from dsutils.logtools import get_logger

logger = get_logger(__name__)


class AdvertQualityAudit:
    """Advert Quality Validation Class"""

    def __init__(
        self,
        rundate: str,
        route: str,
        ad_item_num_threshold: int = 10,
        item_perc_coverage_threshold: float = 0.75,
        theme_coverage_threshold: float = 0.5,
        ad_image_items_match_threshold: float = 0.7,
    ):
        self.rundate: str = rundate
        self.route: str = route
        self.ad_item_num_threshold: int = ad_item_num_threshold
        self.item_perc_coverage_threshold: float = item_perc_coverage_threshold
        self.theme_coverage_threshold: float = theme_coverage_threshold
        self.ad_image_items_match_threshold: float = (
            ad_image_items_match_threshold
        )
        self.control_sheet_tbl: str = ""
        self.ad_item_tbl: str = ""
        self.item_theme_tbl: str = ""

        self.ad_number_items: DataFrame | None = None
        self.identified_items: DataFrame | None = None
        self.advert_item_themes: DataFrame | None = None
        self.best_themes_from_items: DataFrame | None = None
        self.assigned_theme_quality: DataFrame | None = None
        self.image_item_data_quality: DataFrame | None = None
        self.validation_results: DataFrame | None = None
        self.validate_audit_variables

    @property
    def validate_audit_variables(self):
        """Validation of the correct variable types being provided"""
        if self.route not in {"v1", "v2"}:
            raise ValueError("route must be 'v1' or 'v2'")

    @property
    def validation_descriptors(self) -> dict:
        """Descriptors for each failure type evaluated in the process"""
        return {
            "min_item_number_met": "Insufficient items in advert",
            "min_perc_item_identified_match_met": "Insufficient advert items matched to theme items-data validation required",
            "perc_theme_occurrence_met": "Insufficient items in advert matching advert assigned theme",
            "perc_top20_theme_occurrence_met": "Insufficient items in top 20 matching advert assigned theme",
            "perc_primary_theme_occurrence_met": "Insufficient items primary theme matching advert assigned theme",
            "perc_top20_primary_theme_occurrence_met": "Insufficient items primary theme in top 20 matching advert assigned theme",
            "met_ad_image_items_matched_perc_threshold": "Insufficient advert image items matching advert results items",
            "met_ad_image_items_top_ranked_matched_perc_threshold": "Insufficient advert image items matching top ranked advert results items",
        }

    def resolve_quality_audit_tables(self, config):
        """Resolution of the table names for the tables needed for the audit run"""
        logger.info(f"Resolving Quality Audit tables for {self.route}")
        self.item_theme_tbl = config.tables_write.item_themes_latest
        if self.route == "v1":
            self.control_sheet_tbl = config.tables_write.control_sheet_latest
            self.ad_item_tbl = config.tables_read.next_ads_sort_order_latest
        elif self.route == "v2":
            self.control_sheet_tbl = (
                config.tables_write.control_sheet_latest_v2
            )
            self.ad_item_tbl = config.tables_write.sort_order_v2_latest
        else:
            raise ValueError("--route must be either v1 or v2")

    def validate_audit_tables_date(self, spark: SparkSession):
        """Validation of the audit tables to ensure rundates align"""
        tables = [
            self.item_theme_tbl,
            self.control_sheet_tbl,
            self.ad_item_tbl,
        ]

        logger.info(f"Validating Quality Audit tables for {self.route}")
        rundate = datetime.fromisoformat(self.rundate).date()
        for table in tables:
            table_rundate = (
                spark.table(table).select("rundate").distinct().collect()[0][0]
            )
            if not table_rundate:
                raise ValueError(f"No data in table {table}")

            if rundate - table_rundate == timedelta(days=1):
                logger.warning(
                    f"Rundate mismatch in table:{table}, reference_date:{rundate}, table date: {table_rundate}"
                )

            elif rundate - table_rundate != timedelta(days=0):
                raise ValueError(
                    f"Rundate mismatch in table:{table}, reference_date:{rundate}, table date: {table_rundate}"
                )

    def advert_item_identification(self, spark: SparkSession):
        """Determine the number of items behind each provided advert"""
        logger.info("Auditing the number of items returned for adverts")
        control_sheet = spark.table(self.control_sheet_tbl).select(
            "UniqueAdId", "Themes"
        )
        sort_order = spark.table(self.ad_item_tbl)

        self.ad_number_items = (
            sort_order.join(control_sheet.alias("c"), on="UniqueAdID")
            .withColumn("Ad_assigned_theme", F.lower(F.col("c.Themes")))
            .groupBy(F.col("UniqueAdID"), F.col("Ad_assigned_theme"))
            .agg(F.count("*").alias("Total_items"))
            .withColumn(
                "min_item_number_met",
                F.when(
                    F.col("Total_items") >= self.ad_item_num_threshold, True
                ).otherwise(False),
            )
        )

    def advert_items_associated_themes(self, spark: SparkSession):
        """Determine the top themes associated for an adverts items.
        Determine the proportion of items behind an ad in themes data.
        Determine the proportion of items in an advert that are
        associated to the adverts assigned theme
        """
        # Get all themes for the advert items
        item_themes = spark.table(self.item_theme_tbl)
        sort_order = spark.table(self.ad_item_tbl)

        if not self.ad_number_items:
            raise ValueError("No advert items data to validate item themes")
        items_col = (
            F.col("so.item") if self.route == "v2" else F.col("so.items")
        )
        combined_agg = (
            sort_order.alias("so")
            .join(
                item_themes.alias("it"),
                items_col == F.col("it.pid"),
                how="inner",
            )
            .withColumn(
                "top_20",
                F.when(F.col("item_pos") <= 20, F.lit(1)).otherwise(F.lit(0)),
            )
            .withColumn(
                "primary_theme",
                F.when(F.col("theme_rank") == 1, F.lit(1)).otherwise(F.lit(0)),
            )
            .withColumn(
                "top20_primary_theme",
                F.when(
                    (F.col("theme_rank") == 1) & (F.col("item_pos") <= 20),
                    F.lit(1),
                ).otherwise(F.lit(0)),
            )
        )

        # Get overall coverage of the items in themes
        self.identified_items = (
            combined_agg.join(self.ad_number_items, on="UniqueAdID")
            .groupBy(F.col("UniqueAdID"), F.col("Total_items"))
            .agg(F.countDistinct(F.col("pid")).alias("number_items_"))
            .withColumn(
                "percentage_item_identified_match",
                F.round(F.col("number_items_") / F.col("Total_items"), 2),
            )
            .withColumn(
                "min_perc_item_identified_match_met",
                F.when(
                    F.col("percentage_item_identified_match")
                    >= self.item_perc_coverage_threshold,
                    True,
                ).otherwise(False),
            )
            .select(
                F.col("UniqueAdID"),
                F.col("percentage_item_identified_match"),
                F.col("min_perc_item_identified_match_met"),
            )
        )

        # Get overall occurrences of the themes for advert items
        self.advert_item_themes = (
            combined_agg.groupBy(F.col("so.UniqueAdID"), F.col("it.theme"))
            .agg(
                F.count("pid").alias("total_occurrence"),
                F.sum(F.col("top_20")).alias("top20_occurrence"),
                F.sum(F.col("primary_theme")).alias(
                    "primary_theme_occurrence"
                ),
                F.sum(F.col("top20_primary_theme")).alias(
                    "top20_primary_theme_occurrence"
                ),
            )
            .withColumn(
                "total_occurrence_rank",
                F.row_number().over(
                    Window.partitionBy(F.col("UniqueAdID")).orderBy(
                        F.desc(F.col("total_occurrence"))
                    )
                ),
            )
            .withColumn(
                "top20_occurrence_rank",
                F.row_number().over(
                    Window.partitionBy(F.col("UniqueAdID")).orderBy(
                        F.desc(F.col("top20_occurrence"))
                    )
                ),
            )
            .withColumn(
                "primary_theme_rank",
                F.row_number().over(
                    Window.partitionBy(F.col("UniqueAdID")).orderBy(
                        F.desc(F.col("primary_theme_occurrence"))
                    )
                ),
            )
            .withColumn(
                "top20_primary_theme_rank",
                F.row_number().over(
                    Window.partitionBy(F.col("UniqueAdID")).orderBy(
                        F.desc(F.col("top20_primary_theme_occurrence"))
                    )
                ),
            )
        )

        ##Get the best theme for the different views of the advert items
        columns = {
            "total_occurrence_rank": "total_occurrence",
            "top20_occurrence_rank": "top20_occurrence",
            "primary_theme_rank": "primary_theme_occurrence",
            "top20_primary_theme_rank": "top20_primary_theme_occurrence",
        }

        assigned_ads = self.ad_number_items.select(
            "UniqueAdID", "Total_items"
        ).distinct()

        for column, alias in columns.items():
            temp = (
                self.advert_item_themes.filter(F.col(column) == F.lit(1))
                .groupBy("UniqueAdID", "theme")
                .pivot(column)
                .agg(F.first(alias))
                .withColumnsRenamed(
                    {
                        "theme": f"highest_{alias}_theme",
                        "1": f"highest_{alias}_perc",
                    }
                )
            )

            divider = F.lit(20) if "top20" in column else F.col("Total_items")
            assigned_ads = assigned_ads.join(
                temp, on="UniqueAdID", how="left"
            ).withColumn(
                f"highest_{alias}_perc",
                F.round(F.col(f"highest_{alias}_perc") / divider, 2),
            )

        

        self.best_themes_from_items = assigned_ads.drop("Total_items")

        self.assigned_theme_quality = (
            self.ad_number_items.alias("so")
            .join(
                self.advert_item_themes.alias("ct"),
                on=(
                    (F.col("so.UniqueAdID") == F.col("ct.UniqueAdID"))
                    & (F.col("so.Ad_assigned_theme") == F.col("ct.theme"))
                ),
                how="left",
            )
            .withColumn(
                "percentage_theme_occurrence",
                F.round(F.col("total_occurrence") / F.col("Total_items"), 2),
            )
            .withColumn(
                "percentage_top20_theme_occurrence",
                F.round(F.col("top20_occurrence") / F.lit(20), 2),
            )
            .withColumn(
                "percentage_primary_theme_occurrence",
                F.round(
                    F.col("primary_theme_occurrence") / F.col("Total_items"), 2
                ),
            )
            .withColumn(
                "percentage_top20_primary_theme_occurrence",
                F.round(
                    F.col("top20_primary_theme_occurrence") / F.lit(20), 2
                ),
            )
            .withColumn(
                "perc_theme_occurrence_met",
                F.when(
                    F.col("percentage_theme_occurrence")
                    >= self.theme_coverage_threshold,
                    True,
                ).otherwise(False),
            )
            .withColumn(
                "perc_top20_theme_occurrence_met",
                F.when(
                    F.col("percentage_top20_theme_occurrence")
                    >= self.theme_coverage_threshold,
                    True,
                ).otherwise(False),
            )
            .withColumn(
                "perc_primary_theme_occurrence_met",
                F.when(
                    F.col("percentage_primary_theme_occurrence")
                    >= self.theme_coverage_threshold,
                    True,
                ).otherwise(False),
            )
            .withColumn(
                "perc_top20_primary_theme_occurrence_met",
                F.when(
                    F.col("percentage_top20_primary_theme_occurrence")
                    >= self.theme_coverage_threshold,
                    True,
                ).otherwise(False),
            )
            .select(
                F.col("so.UniqueAdID"),
                F.coalesce(
                    F.col("percentage_theme_occurrence"), F.lit(0)
                ).alias("percentage_theme_occurrence"),
                F.coalesce(
                    F.col("percentage_top20_theme_occurrence"), F.lit(0)
                ).alias("percentage_top20_theme_occurrence"),
                F.coalesce(
                    F.col("percentage_primary_theme_occurrence"), F.lit(0)
                ).alias("percentage_primary_theme_occurrence"),
                F.coalesce(
                    F.col("percentage_top20_primary_theme_occurrence"),
                    F.lit(0),
                ).alias("percentage_top20_primary_theme_occurrence"),
                F.col("total_occurrence_rank"),
                F.col("top20_occurrence_rank"),
                F.col("primary_theme_rank"),
                F.col("top20_primary_theme_rank"),
                F.col("perc_theme_occurrence_met"),
                F.col("perc_top20_theme_occurrence_met"),
                F.col("perc_primary_theme_occurrence_met"),
                F.col("perc_top20_primary_theme_occurrence_met"),
            )
        )

    def advert_image_items_position_validation(self, spark: SparkSession):
        """Determine the items on the advert image position and coverage
        in the landing page results
        """
        sort_order = spark.table(self.ad_item_tbl)
        control_sheet = spark.table(self.control_sheet_tbl)

        ad_image_items = (
            control_sheet.select("UniqueAdID", "Items")
            .distinct()
            .withColumn("item", F.explode(F.split(F.col("Items"), " ")))
        )
        items_col = (
            F.col("so.item") if self.route == "v2" else F.col("so.items")
        )
        item_pos = (
            ad_image_items.alias("ai")
            .join(
                sort_order.alias("so"),
                on=(F.col("so.UniqueAdID") == F.col("ai.UniqueAdID"))
                & (F.col("ai.item") == items_col),
                how="inner",
            )
            .withColumn(
                "top_ranked_number",
                F.when("so.item_pos" <= F.lit(20), F.lit(1)).otherwise(
                    F.lit(0)
                ),
            )
            .groupBy("ai.UniqueAdID")
            .agg(
                F.sum("top_ranked_number").alias(
                    "number_top_ranked_ad_image_items"
                ),
                F.count("*").alias("number_ad_image_items_matched"),
            )
        )

        self.image_item_data_quality = (
            ad_image_items.groupBy("UniqueAdID")
            .agg(F.countDistinct("item").alias("number_ad_image_items"))
            .join(item_pos, on="UniqueAdID", how="left")
            .withColumn(
                "number_ad_image_items_matched",
                F.coalesce(F.col("number_ad_image_items_matched"), F.lit(0)),
            )
            .withColumn(
                "number_top_ranked_ad_image_items",
                F.coalesce(
                    F.col("number_top_ranked_ad_image_items"), F.lit(0)
                ),
            )
            .withColumn(
                "perc_ad_image_items_matched",
                F.round(
                    F.col("number_ad_image_items_matched")
                    / F.col("number_ad_image_items"),
                    2,
                ),
            )
            .withColumn(
                "perc_ad_image_items_matched_top_ranked",
                F.round(
                    F.col("number_top_ranked_ad_image_items")
                    / F.col("number_ad_image_items"),
                    2,
                ),
            )
            .withColumn(
                "met_ad_image_items_matched_perc_threshold",
                F.when(
                    F.col("perc_ad_image_items_matched")
                    >= self.ad_image_items_match_threshold,
                    True,
                ).otherwise(False),
            )
            .withColumn(
                "met_ad_image_items_top_ranked_matched_perc_threshold",
                F.when(
                    F.col("perc_ad_image_items_matched_top_ranked")
                    >= self.ad_image_items_match_threshold,
                    True,
                ).otherwise(False),
            )
        )

    def format_validation_results(self, spark: SparkSession):
        """Combining all of the validation datasets results,
        including overall validation field flag & descriptors
        for validation failures
        """
        final_results = (
            self.ad_number_items.join(
                self.assigned_theme_quality, on="UniqueAdID", how="left"
            )
            .join(self.identified_items, on="UniqueAdID", how="left")
            .join(self.best_themes_from_items, on="UniqueAdID", how="left")
            .join(self.image_item_data_quality, on="UniqueAdID", how="left")
            .withColumn("process_version", F.lit(self.route))
        )

        validation_dict = self.validation_descriptors
        target_cols = [
            col
            for col in validation_dict.keys()
            if col in final_results.columns
        ]
        label_expressions = [
            F.when(
                ~F.coalesce(F.col(col), F.lit(False)), F.lit(val)
            ).otherwise(None)
            for col, val in validation_dict.items()
            if col in final_results.columns
        ]

        final_results = final_results.withColumn(
            "Validated",
            ~F.array_contains(
                F.array(
                    *[F.coalesce(F.col(c), F.lit(False)) for c in target_cols]
                ),
                False,
            ),
        ).withColumn(
            "ValidationReasons", F.concat_ws(" | ", *label_expressions)
        )

        self.evaluate_validation_results(final_results)

        return final_results

    def evaluate_validation_results(self, df: DataFrame):
        """Evaluation & reporting of the validation overall results metrics"""
        logger.info("Running Validation Metrics on Advert Quality Audit")
        unique_adverts_assessed = df.select("UniqueAdID").distinct().count()
        total_records = df.select("UniqueAdID").count()
        number_ads_passed_validation = (
            df.filter(F.col("Validated"))
            .select("UniqueAdID")
            .distinct()
            .count()
        )
        number_ads_failed_validation = (
            unique_adverts_assessed - number_ads_passed_validation
        )
        if total_records != unique_adverts_assessed:
            logger.warning("Adverts not unique in Quality Audit results")
        logger.info(
            f"Total Adverts assessed for {self.route}: {unique_adverts_assessed}"
        )
        logger.info(
            f"Adverts Passed Advert Quality Audit: {number_ads_passed_validation}"
        )
        logger.info(
            f"Adverts Failed Advert Quality Audit: {number_ads_failed_validation}"
        )

    def run_all_validation_checks(self, spark: SparkSession):
        """Execution of all validation steps for Advert Quality Audit"""
        self.advert_item_identification(spark)
        self.advert_items_associated_themes(spark)
        self.advert_image_items_position_validation(spark)
