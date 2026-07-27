import mlflow.pyfunc
from concurrent.futures import ThreadPoolExecutor

import logging
import psycopg2
import pandas as pd

logger = logging.getLogger("realtime_reranking_model")


class RealtimeKnownRerankingModel(mlflow.pyfunc.PythonModel):
    def __init__(self):
        self.conn = None
        self.min_number_of_ads = 10

    def load_context(self):
        """Initialise the context at the point the model is loaded"""
        # TODO: Resolve this so is getting oauth token
        # TODO: if not connecting has retries for reconnecting
        pass

    def generate_lakebase_connection(self):
        #     #TODO UPDATE HOW THIS WILL WORK FOR A SERVED MODEL
        conn = psycopg2.connect(
            host=config.ONLINE_RT_LAKEBASE_HOST,
            dbname=config.ONLINE_RT_FEATURE_STORE_DB_NAME,
            user=config.ONLINE_RT_FEATURE_STORE_USER,
            password=config.ONLINE_RT_FEATURE_STORE_PASSWORD,
            sslmode="require",
        )
        self.conn = conn

    def run_query(self, qry):
        results = pd.DataFrame()
        try:
            if not self.conn:
                self.generate_lakebase_connection()
            with self.conn.cursor() as cursor:
                cursor.execute(qry)
                colnames = [desc[0] for desc in cursor.description]
                results = cursor.fetchall()
                if results:
                    results = pd.DataFrame(results, columns=colnames)
        except Exception as e:
            logger.error(f"Exception in fetching data: {e}")
        return results

    def predict(self, model_input):

        output = {}
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

        if ad_df.empty | item_df.empty:
            logger.error("No customer ads data or items data found")
            return output

        cross_data = self.realtime_reranking_item_cross(ad_df, item_df)
        if cross_data.empty:
            logger.error("No results from crossed data")
            return output

        best_ad_df = cross_data[cross_data["AdjustedRanking"] == 1]
        results = self.incrementality(best_ad_df)

        if results.empty:
            logger.error("No incrementality results generated")
            return output
        # TODO imporove this
        # final formatting
        number_records = results.groupby("PageType", as_index=False).agg(
            {"UniqueAdID": "count"}
        )
        missing_records = number_records[
            number_records["UniqueAdID"] < self.min_number_of_ads
        ]

        if not missing_records.empty:
            logger.info("Insufficient records for PageTypes")
            # TODO: take results for each PageType if &  combine with records from cross data. (no dups)

        output = self.upload_format(results)

        self.write_to_bloomreach(output)

        return output

    def realtime_reranking_advert_data_prep(self, input_rpid, PageTypeFilters):

        customer_ads = pd.DataFrame()

        if not input_rpid:
            logger.info("No input RPID provided")
            return customer_ads

        # TODO: migrate to centralised functionality
        table = "marketingdata_dev.claire_wilsonbarnes.next_uk_nextads_realtime_reranking_preranked_ads_sample_online"

        ads_qry = f"""
            SELECT
                *
            FROM {table}
            WHERE
                roamingprofileid={input_rpid}
            ;
            """
        preranked_ads = self.run_query(ads_qry)
        preranked_ads_df = pd.DataFrame(preranked_ads)
        customer_ads = preranked_ads_df[
            preranked_ads_df["PageType"].isin(PageTypeFilters)
        ]

        if customer_ads.empty:
            logger.error("No current ads found for location")

        return customer_ads

    def realtime_reranking_item_data_prep(self, input_features: dict):

        items_data = pd.DataFrame()
        flattened_data = [{**value} for value in input_features.values()]
        input_data = pd.DataFrame(flattened_data).rename(
            columns={"item": "pid"}
        )
        input_data["pid"] = input_data["pid"].str.upper()

        if input_data.empty:
            logger.error("No items identified")
            return items_data

        # TODO change to variable
        table = "marketingdata_dev.claire_wilsonbarnes.next_uk_nextads_realtime_reranking_item_weighting_rules_online"

        input_pids = "', '".join(input_data["pid"].unique())

        items_qry = f"""
            SELECT
                *
            FROM {table}
            WHERE
                pid IN ('{input_pids}')
            ;
            """
        products_data = self.run_query(items_qry)

        items_data = products_data.merge(
            input_data, on=["pid", "action"], how="inner"
        ).drop_duplicates()

        if items_data.empty:
            logger.error("Items not found in dataset")

        return items_data

    # 3 Cross item table with weighting factors

    def realtime_reranking_item_cross(self, customer_ads, item_df):

        # 4 Cross weighting factors with batch adverts
        cols = ["brand", "next_category", "department", "prem_level_brand"]
        customer_ads_ = customer_ads[
            ["UniqueAdID", "PageType", "Score", "TriggerScore"]
        ]
        customer_ads_["adjusted_weighting_final"] = 0

        for col in cols:
            cross_df = customer_ads.merge(
                item_df[item_df[f"weighting_{col}"] != 0],
                on=col,
                how="inner",
            )
            cross_df["adjusted_weighting"] = (
                cross_df[f"weighting_{col}"] * cross_df[f"{col}_perc_coverage"]
            )
            cross_df = cross_df.groupby(
                ["UniqueAdID", "PageType"], as_index=False
            ).sum("adjusted_weighting")[
                ["UniqueAdID", "PageType", "adjusted_weighting"]
            ]

            customer_ads_ = (
                customer_ads_.merge(
                    cross_df, on=["UniqueAdID", "PageType"], how="left"
                )
                .groupby(
                    [
                        "UniqueAdID",
                        "PageType",
                        "Score",
                        "TriggerScore",
                        "adjusted_weighting_final",
                    ],
                    as_index=False,
                )
                .sum("adjusted_weighting")
                .rename(columns={"adjusted_weighting": "combined_weight"})
            )
            customer_ads_["adjusted_weighting_final"] = (
                customer_ads_["adjusted_weighting_final"]
                + customer_ads_["combined_weight"]
            )
            customer_ads_ = customer_ads_.drop(columns=["combined_weight"])

        customer_ads_["adjusted_weighting_final"] = (
            customer_ads_["adjusted_weighting_final"] + 1
        )
        customer_ads_["AdjustedTriggerScore"] = (
            customer_ads_["TriggerScore"]
            * customer_ads_["adjusted_weighting_final"]
        )

        customer_ads_["AdjustedRanking"] = customer_ads_.groupby(
            ["PageType"], as_index=False
        )[["AdjustedTriggerScore", "TriggerScore", "UniqueAdID"]].rank(
            method="dense", ascending=False
        )["AdjustedTriggerScore"]
        customer_ads_["OriginalRanking"] = customer_ads_.groupby(
            ["PageType"], as_index=False
        )[["TriggerScore", "UniqueAdID"]].rank(
            method="dense", ascending=False
        )["TriggerScore"]

        return customer_ads_[
            [
                "UniqueAdID",
                "PageType",
                "Score",
                "TriggerScore",
                "AdjustedTriggerScore",
                "AdjustedRanking",
                "OriginalRanking",
                "adjusted_weighting_final",
            ]
        ]

    def incrementality(self, best_ad_df):

        cust_affinity = pd.DataFrame()
        # TODO change to variable
        table = "marketingdata_dev.claire_wilsonbarnes.next_uk_nextads_advert_advert_association_online"

        # 2 Filter item table to provided best adverts for each location
        best_ads = "', '".join(best_ad_df["UniqueAdID"].unique())
        ad_affinity_qry = f"""SELECT
                            *
                            FROM {table}
                            WHERE "ViewUniqueAdID" IN ('{best_ads}')
                        ;"""

        ad_affinity_df = self.run_query(ad_affinity_qry)
        if ad_affinity_df.empty:
            logger.error("No affinity data identified")
            return cust_affinity

        merged = ad_affinity_df.merge(
            best_ad_df,
            left_on="ViewUniqueAdID",
            right_on="UniqueAdID",
            how="inner",
        )
        col_idx = merged.columns.get_indexer(merged["PageType"])
        is_page_type_true = pd.Series(
            merged.values[np.arange(len(merged)), col_idx], index=merged.index
        ).astype(bool)
        cust_affinity = merged[is_page_type_true][
            [
                "PageType",
                "UniqueAdID",
                "AtbUniqueAdID",
                "lift_adjusted",
                "AdjustedTriggerScore",
            ]
        ]

        return cust_affinity

    def upload_format(self, results_df):
        # TODO: set up format
        # format the items for the upload structure
        pass

    def write_to_bloomreach(self):
        # TODO Add in additional step here to write out to bloommreach API
        pass
