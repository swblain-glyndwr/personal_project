import mlflow.pyfunc
from concurrent.futures import ThreadPoolExecutor
import time
import os
from databricks.sdk import WorkspaceClient
import hashlib
import json
import logging
import psycopg2
import numpy as np
import pandas as pd

logger = logging.getLogger("realtime_reranking_model")


class RealtimeKnownRerankingModel(mlflow.pyfunc.PythonModel):
    def load_context(self, context: dict):

        # Connection context
        self.db_host = os.environ.get("NEXTADS_REALTIME_ONLINE_LAKEBASE_HOST")
        self.db_port = os.environ.get(
            "NEXTADS_REALTIME_ONLINE_LAKEBASE_PORT", "5432"
        )
        self.db_name = os.environ.get(
            "NEXTADS_REALTIME_ONLINE_LAKEBASE_DATABASE", "databricks_postgres"
        )
        self.db_user = os.environ.get("NEXTADS_REALTIME_ONLINE_LAKEBASE_USER")
        self.workspace_client = WorkspaceClient(
            host=os.environ.get("NEXTADS_REALTIME_ONLINE_LAKEBASE_HOST"),
            client_id=os.environ.get(
                "NEXTADS_REALTIME_ONLINE_LAKEBASE_CLIENT_ID"
            ),
            client_secret=os.environ.get(
                "NEXTADS_REALTIME_ONLINE_LAKEBASE_CLIENT_SECRET"
            ),
        )
        # Token caching state
        self._cached_token = None
        self._token_expiry = 0
        self.conn = None

        self.table_catalog:str =os.environ.get(
            "NEXTADS_REALTIME_ONLINE_LAKEBASE_CATALOG", "marketingdata_dev"
        )
        self.table_schema:str =os.environ.get(
                    "NEXTADS_REALTIME_ONLINE_LAKEBASE_SCHEMA", "nextads"
                )
        
        # Model Variables
        self.pagetype_filter: list = context.get(
            "pagetype_filter", ["ProductListingPage", "ShoppingBag"]
        )
        self.min_number_of_ads: int = context.get("min_number_of_ads", 10)
        self.item_feature_columns: list = [
            "brand",
            "next_category",
            "department",
            "prem_level_brand",
        ]
        # Payload settings
        self.ad_fatigue_threshold: int = context.get("ad_fatigue_threshold", 2)
        self.ad_fatigue_active_locations: list = context.get(
            "ad_fatigue_active_locations", []
        )
        self.trigger_record_limit: int = context.get("trigger_record_limit", 5)
        self.fragments_record_limit: int = context.get(
            "fragment_record_limit", 20
        )

        # TODO: add Audience column in here!
        self.customer_cells_columns: list = [
            "AccountNumber",
            "roamingprofileid",
            "FallowControl",
            "ShoppingBagTest1",
            "OrderCompleteTest1",
            "LandingPageTest1",
            "AdHocABTest1",
            "AdHocABTest2",
            "AdHocABTest3",
            "AdHocABTest4",
            "AdHocABTest5",
            "AdHocABTest6",
            "AdHocABTest7",
            "AdHocABTest8",
            "AdHocABTest9",
            "ChampionChallenger",
            "PageTypeIsolation",
            "specialaccountindicator",
            "AlgoDivision",
            # "Audience",
            "IsPremium",
        ]
        # Settings for experiments
        # Default set to False so no impact to customer experience if there were to be an issue
        self.control: bool = False
        self.control_value: str = "NoAds"
        self.experiment_settings: dict = {
            "experiments": {
                "NextAds": "FallowControl",
                "PageIsolation": "PageTypeIsolation",
                "NextGenAds": "AdHocABTest1",
            },
            "audience_experiments": {
                "enabled": True,
                "split_col": "Audience",
                "sample": ["Best"],
                "split": None,
            },
        }
        self.experiment_details = {
            "NextAds": [
                ("FallowControl", "NoAds", "CT"),
                ("ShoppingBagTest1", "Basic", "BA"),
                ("ShoppingBagTest1", "Best", "BE"),
            ],
            "PageIsolation": [
                ("split_col", "AllPages", "AP"),
                ("split_col", "PLP_Only", "PL"),
                ("split_col", "SB_Only", "SB"),
                ("split_col", "HP_Only", "HP"),
                ("split_col", "OC_Only", "OC"),
            ],
            # Default behavior for any unlisted col_name (A/B testing)
            "DEFAULT": [
                ("split_col", "A", "A"),
                ("split_col", "B", "B"),
            ],
        }

    def _get_oauth_token(self):
        """Fetches a new 1-hour OAuth token before expiration."""
        now = time.time()
        # Refresh 5 minutes before expiry (tokens last 3600 seconds)
        if not self._cached_token or (self._token_expiry - now) < 300:
            # Generate OAuth token using Databricks SDK
            token_info = self.workspace_client.tokens.create(
                comment="NextAds Realtime Known Reranking Model Lakebase Serving Token",
                lifetime_seconds=3600,
            )
            self._cached_token = token_info.token_value
            self._token_expiry = now + 3600

        return self._cached_token

    def generate_lakebase_connection(self):

        self._get_oauth_token()

        conn = psycopg2.connect(
            host= self.db_host,
            dbname= self.db_name,
            port=self.db_port,
            user= self.db_user,
            password=self._cached_token,
            sslmode="require",
        )
        self.conn = conn

    def run_query(self, qry: str) -> pd.DataFrame:

        results = pd.DataFrame()
        try:
            if not self.conn or self._token_expiry - time.time() < 300:
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

    def predict(self, model_input: dict) -> dict:

        with ThreadPoolExecutor(max_workers=2) as executor:
            ads = executor.submit(
                self.realtime_reranking_advert_data_prep,
                model_input.get("rpid", ""),
                self.pagetype_filter,
            )
            items = executor.submit(
                self.realtime_reranking_item_data_prep,
                model_input.get("items", {}),
            )

        ad_df = ads.result()
        item_df = items.result()

        if ad_df.empty | item_df.empty:
            logger.error("No data identified for items or rpid")
            return {}

        cross_data = self.realtime_reranking_item_cross(ad_df, item_df)
        if cross_data.empty:
            logger.error("No cross data")
            return {}

        best_ad_df = cross_data[cross_data["AdjustedRanking"] == 1]
        results = self.incrementality(best_ad_df)

        if results.empty:
            logger.error("No incrementality results found")
            # TODO: decide on action here

        # TODO: Finish/Improve the final formatting
        number_records = results.groupby("PageType", as_index=False).agg(
            {"UniqueAdID": "count"}
        )
        missing_records = number_records[
            number_records["UniqueAdID"] < self.min_number_of_ads
        ]

        if not missing_records.empty:
            logger.info("Insufficient records for PageTypes")
            # TODO take results for each PageType if &  combine with records from cross data. (no dups)

        customer_cells = ad_df[self.customer_cells_columns].drop_duplicates()

        payload = self.build_payload_structure(customer_cells, results)

        if not payload:
            logger.error("No payload generated")
            return {}

        # TODO write to API

        return payload

    def realtime_reranking_advert_data_prep(
        self, input_rpid: int, page_type_filter: list
    ) -> pd.DataFrame:

        customer_ads = pd.DataFrame()

        if not input_rpid:
            logger.info("No input RPID provided")
            return customer_ads

        table = f"{self.table_catalog}.{self.table_schema}.next_uk_nextads_realtime_reranking_preranked_ads_sample_online"

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
            preranked_ads_df["PageType"].isin(page_type_filter)
        ]

        if customer_ads.empty:
            logger.error("No current ads found for location")

        return customer_ads

    def realtime_reranking_item_data_prep(
        self, input_features: dict
    ) -> pd.DataFrame:

        items_data = pd.DataFrame()
        flattened_data = [{**value} for value in input_features.values()]
        input_data = pd.DataFrame(flattened_data).rename(
            columns={"item": "pid"}
        )
        input_data["pid"] = input_data["pid"].str.upper()

        if input_data.empty:
            logger.error("No items identified")
            return items_data

        table = f"{self.table_catalog}.{self.table_schema}.next_uk_nextads_realtime_reranking_item_weighting_rules_online"

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

    def realtime_reranking_item_cross(
        self, customer_ads: pd.DataFrame, item_df: pd.DataFrame
    ) -> pd.DataFrame:

        customer_ads["adjusted_weighting_final"] = 0
        customer_ads_ = customer_ads[
            [
                "roamingprofileid",
                "UniqueAdID",
                "PageType",
                "Score",
                "TriggerScore",
                "adjusted_weighting_final",
            ]
        ]

        for col in self.item_feature_columns:
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
                "roamingprofileid",
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

    def incrementality(self, best_ad_df: pd.DataFrame) -> pd.DataFrame:

        cust_affinity = pd.DataFrame()

        table = f"{self.table_catalog}.{self.table_schema}.next_uk_nextads_advert_advert_association_online"

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
                "roamingprofileid",
                "PageType",
                "UniqueAdID",
                "AtbUniqueAdID",
                "lift_adjusted",
                "AdjustedTriggerScore",
            ]
        ]

        return cust_affinity

    ## Functions for bulding the payload:

    def determine_control(self, df: pd.DataFrame):
        if df["FallowControl"] == self.control_value:
            self.control = True

    def build_triggers(self, df: pd.DataFrame):
        # Get top scores for ads for the customer
        triggers = []
        ##TODO: SWITCH TO CMSPAGEID RATHER THAN ADID!
        combined = (
            df.groupby(["roamingprofileid", "AtbUniqueAdID"], as_index=False)
            .max("lift_adjusted")
            .sort_values("lift_adjusted", ascending=False)
            .rename(columns={"lift_adjusted": "t", "AtbUniqueAdID": "id"})
        )
        triggers = combined[: self.trigger_record_limit][["t", "id"]].to_dict(
            "records"
        )
        return triggers

    def build_adshash(self, dict_data: dict):
        # Build the ad hash for the experiment
        try:
            string_data = json.dumps(dict_data, sort_keys=True)
            return hashlib.sha256(string_data.encode("utf-8")).hexdigest()
        except Exception as e:
            logger.error(f"Error building ad hash {e}")
            return ""

    def build_fragments(self, df):
        # Get top scored ads for the locations
        fragments = []
        pages = df["PageType"].unique().tolist()
        for page in pages:
            # TODO SWAP AtbUniqueAdID to CMSPAGEID
            fragment_ids = (
                df[df["PageType"] == page]
                .sort_values("lift_adjusted", ascending=False)[
                    : self.fragments_record_limit
                ]["AtbUniqueAdID"]
                .tolist()
            )
            fragments.append(
                {
                    "pageTypes": [page],
                    "enableAdFatigueRotation": page
                    in self.ad_fatigue_active_locations,
                    "fragmentIds": fragment_ids,
                }
            )
        return fragments

    def get_experiment_id(
        self, df: pd.DataFrame, col_name: str, split_col_name="split_col"
    ):

        rules = self.experiment_details.get(
            col_name, self.experiment_details["DEFAULT"]
        )
        conditions = []
        choices = []
        for target_col, val, suffix in rules:
            # Resolve 'split_col' dynamically
            actual_col = (
                split_col_name if target_col == "split_col" else target_col
            )
            # Build boolean mask condition
            conditions.append(df[actual_col] == val)
            choices.append(f"{col_name}_{suffix}")

        default_value = f"{col_name}_Z"
        # Return vectorized evaluated result
        return np.select(conditions, choices, default=default_value).tolist()

    def get_audience_experiment_id(self, customer_cells: pd.DataFrame):

        audience = self.experiment_settings.get("audience_experiments", {})
        split_col = audience.get("split_col", "Audience")
        audience_sample = audience.get("sample", ["Best"])
        audience_split = audience.get("split")
        audience_name = audience.get("name", "Audience")

        audience_value = False
        id = ""
        if split_col in customer_cells.columns:
            audience_value = customer_cells[split_col].values[0] | False

        if (
            not audience_value
            or customer_cells["Fallow_Control"] == self.control_value
        ):
            id = f"Aud_{audience_name}_Z"
        elif (
            customer_cells["ShoppingBagTest1"].values[0] not in audience_sample
        ):
            id = f"Aud_{audience_name}_Z"
        elif customer_cells[split_col] == audience_value:
            id = f"Aud_{audience_name}_{audience_split}"
        else:
            id = f"Aud_{audience_name}_Z"

        return id

    def build_experiment_id(self, experiment_df: pd.DataFrame):

        ids = []
        if self.experiment_settings.get("audience_settings", {}).get(
            "enabled", False
        ):
            audience_id = self.get_audience_experiment_id(experiment_df)
            ids.extend(audience_id)
        # Get the list of experiments
        experiments = self.experiment_settings.get("experiments", {})

        for key, value in experiments.items():
            ids.extend(self.get_experiment_id(experiment_df, key, value))

        experiment_id = " | ".join(ids)

        return experiment_id

    def build_payload_structure(
        self, customer_cells_df: pd.DataFrame, results_df: pd.DataFrame
    ) -> dict:

        with ThreadPoolExecutor(max_workers=4) as executor:
            executor.submit(self.determine_control, customer_cells_df)
            triggers = executor.submit(self.build_triggers, results_df)
            experiment_id = executor.submit(
                self.build_experiment_id, customer_cells_df
            )
            fragments = executor.submit(self.build_fragments, results_df)

        payload = {
            "ads": {
                "adFatigueImpressionThreshold": self.ad_fatigue_threshold,
                "experimentId": experiment_id.result(),
                "triggers": triggers.result(),
                "control": self.control,
                "fragments": fragments.result(),
            }
        }

        if self.ad_fatigue_active_locations:
            hash_data = {
                "account_number": customer_cells_df["AccountNumber"].values[0]
            }
            hash_data = hash_data | payload.get("ads", {})
            payload["ads"]["adsHash"] = self.build_adshash(hash_data)

        return payload

    def write_to_bloomreach(self):
        # Multiprocess the steps here!!!
        # TODO Add in additional step here to write out to bloommreach API
        pass
