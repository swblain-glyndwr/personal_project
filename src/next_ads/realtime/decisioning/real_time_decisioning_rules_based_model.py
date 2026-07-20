import mlflow.pyfunc
from concurrent.futures import ThreadPoolExecutor


# # For TESTING
# input_rpid='4209253304'
# #Testing
# input_features= {1 :{"item":"v12037",
#                     "action": "view"},
#                 2:{"item": "w87234",
#                 "action": "view"},
#                 3: {"item": "w03942",
#                     "action":"view"}}

# inputs={"rpid": '4209253304' ,
#         "items": {1 :{"item":"v12037",
#                     "action": "view"},
#                 2:{"item": "w87234",
#                 "action": "view"},
#                 3: {"item": "w03942",
#                     "action":"view"}}}


class AsyncRealtimeRerankingModel(mlflow.pyfunc.PythonModel):
    def predict(self, model_input):

        PageTypeFilters = ["ProductListingPage", "ShoppingBag"]

        with ThreadPoolExecutor(max_workers=2) as executor:
            ads = executor.submit(
                self.realtime_reranking_advert_data_prep,
                model_input.get("rpid", ""),
                PageTypeFilters,
            )
            items = executor.submit(
                self.realtime_reranking_item_data_prep,
                model_input.get("items", {}),
            )

        ad_df = ads.result()
        item_df = items.result()

        if (not ad_df) & (not item_df):
            # TODO Improve this
            logger.error("No data")
            return None
        cross_data = self.realtime_reranking_item_cross(ad_df, item_df)

        results = self.incrementality(cross_data)

        # TODO combine results to output
        final_ads = self.upload_format(results)
        # TODO write to API

        return results

    def realtime_reranking_advert_data_prep(self, input_rpid, PageTypeFilters):

        from pyspark.sql import functions as F

        # default_variables
        account_number = None
        target = False
        # Tables needed
        # TODO: migrate to centralised functionality
        tbls = {
            "rpid_account": "marketingdata_prod.warehouse.rpid_with_accounts",
            "preranked_ads": "marketingdata_prod.warehouse.next_uk_nextads_preranked_ads_from_themes_v2_latest",
            "customer_cells": "marketingdata_prod.warehouse.next_uk_nextads_customer_cells_latest",
            "ad_features": "marketingdata_dev.claire_wilsonbarnes.next_uk_nextads_realtime_reranking_advert_features",
        }

        # 1 find customer
        # TODO this is sooooo slow (10s)
        rpid_table = spark.table(tbls["rpid_account"])

        account_number = (
            (rpid_table.filter(F.col("roamingprofileid") == input_rpid))
            .select(F.col("account_number"))
            .collect()
        )
        if not account_number:
            logger.info("No account found")
            return
        account_number = account_number[0][0]

        # 2 check if they are control or targeted
        # TODO: Add additional flags/checks here for which ads we want to filter to!
        cells_table = spark.table(tbls["customer_cells"])

        customer_details = (
            cells_table.filter(F.col("AccountNumber") == account_number)
            .select(
                F.col("FallowControl"),
                F.col("PageTypeIsolation"),
                F.col("HomePageTest1"),
                F.col("ShoppingBagTest1"),
                F.col("OrderCompleteTest1"),
                F.col("LandingPageTest1"),
            )
            .collect()
        )
        if customer_details:
            target = True if customer_details[0][0] == "Ads" else False
        if not target:
            logger.info("Account not in target group")
            return

        # 3 Filter batch adverts to current customer

        current_ranked_ads = spark.table(tbls["preranked_ads"])
        customer_ads = current_ranked_ads.filter(
            F.col("AccountNumber") == account_number
        ).filter(F.col("PageType").isin(PageTypeFilters))
        if customer_ads.isEmpty():
            logger.error("No current ads found for location")
        # Join advert features to
        ad_features = spark.table(tbls["ad_features"])
        customer_ads = customer_ads.join(
            ad_features, on="UniqueAdID", how="left"
        )

        return customer_ads

    def realtime_reranking_item_data_prep(self, input_features: dict):

        from pyspark.sql import functions as F

        flattened_data = [{**value} for value in input_features.values()]

        # Create Spark DataFrame directly
        input_data = spark.createDataFrame(flattened_data).select(
            F.upper(F.col("item")).alias("pid"),
            F.col("action"),
        )

        # Variables
        items_data = None

        # TODO change to variable
        tbls = {
            "product_table": "marketingdata_dev.claire_wilsonbarnes.next_uk_nextads_realtime_reranking_product_features",
            "weighting_table": "marketingdata_dev.claire_wilsonbarnes.next_uk_nextads_realtime_reranking_rules_weighting",
        }
        product_columns = [
            "pid",
            "action",
            "brand",
            "next_category",
            "department",
            "prem_level_brand",
        ]

        if input_data.isEmpty():
            logger.error("No items identified")
            return
        # 2 Filter item table to provided features

        prod_table = spark.table(tbls["product_table"])
        items_data = (
            prod_table.join(input_data, on="pid", how="inner")
            .select(product_columns)
            .distinct()
        )

        if items_data.isEmpty():
            logger.error("Items not found in dataset")
            return

        # TODO Migrate pivot to daily job
        items_weights = spark.table(tbls["weighting_table"])
        weights = (
            items_weights.groupBy("action", "rundate")
            .pivot("feature")
            .agg(F.first("weight"))
            .na.fill(0)
        )
        weights = weights.select(
            [
                F.col(c).alias(f"weighting_{c}")
                if c not in ("action", "rundate")
                else F.col(c)
                for c in weights.columns
            ]
        )

        combined = items_data.join(
            weights, on="action", how="inner"
        ).withColumn(
            "weighting_prem_level_brand",
            F.when(
                F.col("prem_level_brand"), F.col("weighting_prem_level_brand")
            ).otherwise(F.lit(0)),
        )
        # TODO:Add in validation steps here

        return combined

    # 3 Cross item table with weighting factors

    def realtime_reranking_item_cross(self, customer_ads, combined):

        from pyspark.sql import Window
        from pyspark.sql import functions as F

        # 4 Cross weighting factors with batch adverts
        cols = ["brand", "next_category", "department", "prem_level_brand"]
        customer_ads_ = customer_ads.select(
            "UniqueAdID", "PageType", "Score", "TriggerScore"
        ).withColumn("adjusted_weighting_final", F.lit(0))

        for col in cols:
            cross_df = (
                customer_ads.join(
                    (combined.filter(F.col(f"weighting_{col}") != F.lit(0))),
                    on=col,
                    how="inner",
                )
                .withColumn(
                    "adjusted_weighting",
                    F.col(f"weighting_{col}") * F.col(f"{col}_perc_coverage"),
                )
                .groupBy("UniqueAdID")
                .agg(
                    F.sum(F.col("adjusted_weighting")).alias(
                        "adjusted_weighting"
                    )
                )
            )

            # customer_ads_=(customer_ads_.join(cross_df, on="UniqueAdID", how="left")
            #                .groupBy("UniqueAdID", "PageType", "Score", "TriggerScore")
            #                .agg(F.sum(F.coalesce(F.col("adjusted_weighting"), F.lit(0))).alias("adjusted_weighting")))
            customer_ads_ = (
                customer_ads_.join(cross_df, on="UniqueAdID", how="left")
                .groupBy(
                    "UniqueAdID",
                    "PageType",
                    "Score",
                    "TriggerScore",
                    "adjusted_weighting_final",
                )
                .agg(
                    F.sum(
                        F.coalesce(F.col("adjusted_weighting"), F.lit(0))
                    ).alias("combined_weight")
                )
                .withColumn(
                    "adjusted_weighting_final",
                    F.col("adjusted_weighting_final")
                    + F.col("combined_weight"),
                )
            )

            cust_ad_weighting = (
                customer_ads_.withColumn(
                    "adjusted_weighting_final",
                    F.col("adjusted_weighting_final") + F.lit(1),
                )
                .withColumn(
                    "AdjustedTriggerScore",
                    F.col("TriggerScore") * F.col("adjusted_weighting_final"),
                )
                .withColumn(
                    "AdjustedRanking",
                    F.row_number().over(
                        Window.partitionBy("PAgeType").orderBy(
                            F.desc(F.col("AdjustedTriggerScore")),
                            F.desc(F.col("TriggerScore")),
                            F.desc(F.col("UniqueAdID")),
                        )
                    ),
                )
                .withColumn(
                    "OriginalRanking",
                    F.row_number().over(
                        Window.partitionBy("PageType").orderBy(
                            F.desc(F.col("TriggerScore")),
                            F.desc(F.col("UniqueAdID")),
                        )
                    ),
                )
                .select(
                    "UniqueAdID",
                    "PageType",
                    "Score",
                    "TriggerScore",
                    "AdjustedTriggerScore",
                    "AdjustedRanking",
                    "OriginalRanking",
                    "adjusted_weighting_final",
                )
            )

            return cust_ad_weighting

    def incrementality(self, cust_ad_weighting):

        from pyspark.sql import functions as F

        tbls = {
            "ad_affinity_table": "marketingdata_dev.claire_wilsonbarnes.next_uk_nextads_advert_advert_association",
        }

        ad_affinity = spark.table(tbls["ad_affinity_table"])

        # TODO: what if there is not rank 1 item match?!?!- NO UPDATE?

        cust_affinity = (
            cust_ad_weighting.alias("aw")
            .filter(F.col("AdjustedRanking") == F.lit(1))
            .join(
                ad_affinity.alias("aa"),
                on=(
                    (F.col("aa.ViewUniqueAdID") == F.col("aw.UniqueAdID"))
                    & (ad_affinity[F.col("aw.PageType")])
                ),
                how="inner",
            )
            .select(
                "PageType",
                "AtbUniqueAdID",
                "lift_adjusted",
                "AdjustedTriggerScore",
            )
        )

        return cust_affinity

    def upload_format(self, cross_df):

        # format the items for the upload structure
        pass

    def write_to_bloomreach(self):
        pass
