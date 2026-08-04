import mlflow.pyfunc
from concurrent.futures import ThreadPoolExecutor
import time
import os
from databricks.sdk import WorkspaceClient  ## must be > 0.56 version!!
import logging
import psycopg2
import numpy as np
import pandas as pd
import hashlib
import json


logger = logging.getLogger("realtime_reranking_model")


class NextAdsLakeBaseConnector:
    """Connector class for generating and utilising Databricks Lakebase
    connectors
    """

    def __init__(self):
        # Lakebase connection context
        self.db_host = os.environ.get("NEXTADS_REALTIME_ONLINE_LAKEBASE_HOST")
        self.db_port = os.environ.get(
            "NEXTADS_REALTIME_ONLINE_LAKEBASE_PORT", "5432"
        )
        self.db_name = os.environ.get(
            "NEXTADS_REALTIME_ONLINE_LAKEBASE_DATABASE", "databricks_postgres"
        )
        self.db_endpoint = os.environ.get(
            "NEXTADS_REALTIME_ONLINE_LAKEBASE_ENDPOINT",
            "projects/next-ads-realtime/branches/production/endpoints/primary",
        )
        self.db_user = os.environ.get("NEXTADS_REALTIME_ONLINE_LAKEBASE_USER")
        self.workspace_client = WorkspaceClient()

        # Token caching state
        self._cached_token = None
        self._token_expiry = 0
        self.conn = None

    ## Lakebase connectivity Functions
    def _get_oauth_token(self) -> str:
        """Fetches the OAuth token for connection to the Lakebase.
        Refreshes the token if it is < 5 mins from expiry time otherwise uses current token

        Returns:
        ------
        str:
            The OAuth token
        """
        now = time.time()
        # Refresh 5 minutes before expiry (tokens last 3600 seconds)
        if not self._cached_token or (self._token_expiry - now) < 300:
            # Generate OAuth token using Databricks SDK
            token_info = self.workspace_client.postgres.generate_database_credential(
                endpoint="projects/next-ads-realtime/branches/production/endpoints/primary"
            )

            self._cached_token = token_info.token
            ## Token defaults to 1 hour expiry time
            self._token_expiry = now + 3600

        return self._cached_token

    def generate_lakebase_connection(self) -> None:
        """Generates the Lakebase connection instance using the credentials set in the environment"""
        self._get_oauth_token()

        conn = psycopg2.connect(
            host=self.db_host,
            dbname=self.db_name,
            port=self.db_port,
            user=self.db_user,
            password=self._cached_token,
            sslmode="require",
        )
        self.conn = conn

    def run_query(self, qry: str) -> pd.DataFrame:
        """Executes a PostgreSQL query in the Lakebase and returns the result of the query

        Parameters
        ---------
        qry: str
            The formatted SQL string for the query

        Returns:
        -------
        pd.DataFrame
            dataframe of the output results
            default is an empty dataframe
        """
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


class NextAdsPayloadGenerator:
    """Class for functionality of generating the NextAds payload components
    to enable bloomreach update of nextads property
    """

    def __init__(
        self,
        ad_fatigue_active_locations: list = [],
        ad_fatigue_threshold: int = 2,
        trigger_record_limit: int = 5,
        fragments_record_limit: int = 20,
    ):
        # Payload settings
        self.ad_fatigue_threshold: int = ad_fatigue_threshold
        self.trigger_record_limit: int = trigger_record_limit
        self.fragments_record_limit: int = fragments_record_limit
        self.ad_fatigue_active_locations: list = ad_fatigue_active_locations

        # Experiment Settings
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

    def determine_control(self, df: pd.DataFrame) -> bool:
        """Determine whether or not the customer is a control customer

        Parameters
        ---------
        df: pd.DataFrame
            single row dataframe of customer experiment settings
        """
        return df["FallowControl"].iloc[0] == self.control_value

    def build_triggers(self, df: pd.DataFrame) -> list:
        """Determine the top Advert trigger score levels to be added to google metadata

        Parameters
        ---------
        df: pd.DataFrame
            Ranked advert reccomendation dataframe

        Returns:
        ------
        list:
            list of dictionaries
            example format: {"t": score_value, "id": advertid }
        """
        # Get top scores for ads for the customer
        triggers = []
        combined = (
            df.groupby(["roamingprofileid", "AtbCMSPageID"], as_index=False)
            .max("lift_adjusted")
            .sort_values("lift_adjusted", ascending=False)
            .rename(columns={"lift_adjusted": "t", "AtbCMSPageID": "id"})
        )
        triggers = combined[: self.trigger_record_limit][["t", "id"]].to_dict(
            "records"
        )
        return triggers

    def build_adshash(self, dict_data: dict) -> str:
        """Build a hash of the payload details

        Parameters
        ---------
        dict_data:dict
            The payload details to build the hash from

        Returns:
        ------
        str:
            String of the hash, default=""
        """
        try:
            string_data = json.dumps(dict_data, sort_keys=True)
            return hashlib.sha256(string_data.encode("utf-8")).hexdigest()
        except Exception as e:
            logger.error(f"Error building ad hash {e}")
            return ""

    def build_fragments(self, df: pd.DataFrame) -> list:
        """Generate a list of the top ads for each advert page location

        Parameters
        ---------
        df: pd.DataFrame
            Dataframe of the adjusted adverts & scores for the customer

        Returns:
        -------
        list:
            list of dictionaries consisting of:
                {"pageTypes": [list of page types],
                "enableAdFatigueRotation": bool ,
                "fragmentIds": [list of Advertids] }
        """
        # Get top scored ads for the locations
        fragments = []
        pages = df["PageType"].unique().tolist()
        for page in pages:
            fragment_ids = (
                df[df["PageType"] == page]
                .sort_values("lift_adjusted", ascending=False)[
                    : self.fragments_record_limit
                ]["AtbCMSPageID"]
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
    ) -> list:
        """Generates the experimentID fragment details
        for the customer experiment settings from a given experiment column

        Parameters
        ---------
        df: pd.DataFrame
            single record dataframe of the customer experiment details
        col_name: str
            string of the column name to
        split_col_name:str
            string of the column containing the experiment split conditions

        Returns:
        ------
        list:
            a list of the experiment details
        """
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

    def get_audience_experiment_id(self, customer_cells: pd.DataFrame) -> str:
        """Generate the audience specific experiment string details for the customer

        Parameters
        ---------
        customer_cells: pd.DataFrame
                single record dataframe of the customer experiment details

        Returns:
        ------
        str:
            Audience ExperimentID string for the customer
        """
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

    def build_experiment_id(self, experiment_df: pd.DataFrame) -> str:
        """Generate the full experiment string details for all experiments settings for the customer

        Parameters
        ---------
        experiment_df: pd.DataFrame
                single record dataframe of the customer experiment details

        Returns:
        ------
        str:
            ExperimentID string for the customer
        """
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
        """Generate the full payload structure for returning the results

        Parameters
        ----------
        customer_cells_df: pd.DataFrame
            single record dataframe of the customer experiment details
        results_df: pd.DataFrame
            Dataframe of the adjusted adverts & scores for the customer

        Returns:
        -------
        dict:
            Dictionary of the payload with the following structure
            {"ads": {
                "adFatigueImpressionThreshold": int ,
                "experimentId": str,
                "triggers": list,
                "control": boolean ,
                "fragments": list,
            }}
        """
        control = self.determine_control(customer_cells_df)
        with ThreadPoolExecutor(max_workers=3) as executor:
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
                "control": control,
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


class RealtimeKnownRerankingModel(mlflow.pyfunc.PythonModel):
    """NextAds Model for use in RealTime Known Customer Reranking

    Prerequisites:
    -------------
    1. Lakebase set up with connection details logged as env variables
    Required connection environment parameters:
        * NEXTADS_REALTIME_ONLINE_LAKEBASE_HOST
        * NEXTADS_REALTIME_ONLINE_LAKEBASE_PORT
        * NEXTADS_REALTIME_ONLINE_LAKEBASE_DATABASE
        * NEXTADS_REALTIME_ONLINE_LAKEBASE_ENDPOINT
        * NEXTADS_REALTIME_ONLINE_LAKEBASE_USER
        * NEXTADS_REALTIME_ONLINE_LAKEBASE_CATALOG
        * NEXTADS_REALTIME_ONLINE_LAKEBASE_SCHEMA

    Functionality:
    --------------
    input={"rpid": 12332, "items": {1:{"item": "ab231", "action": "view"},
                                    {1:{"item": "ad2451", "action": "view"}}}
    rt_model=RealtimeKnownRerankingModel()
    rt_model.load_context({})
    rt_model.predict(input)
    """

    def load_context(self, context: dict) -> None:
        """Initial context loading for the model

        Parameters
        ---------
        context: dict
            pagetype_filter: list, default=["ProductListingPage", "ShoppingBagPage"]
                The page types we wish to deploy this on
            min_number_of_ads: int, default= 10
                The minimium number of adverts it should return for each location
                failure to meet this will result in no return
        """
        ## Table Context
        self.table_catalog: str = os.environ.get(
            "NEXTADS_REALTIME_ONLINE_LAKEBASE_CATALOG", "marketingdata_dev"
        )
        self.table_schema: str = os.environ.get(
            "NEXTADS_REALTIME_ONLINE_LAKEBASE_SCHEMA", "nextads"
        )

        # Model Variables
        self.pagetype_filter: list = context.get(
            "pagetype_filter", ["ProductListingPage", "ShoppingBagPage"]
        )
        self.min_number_of_ads: int = context.get("min_number_of_ads", 10)
        self.item_feature_columns: list = [
            "brand",
            "next_category",
            "department",
            "prem_level_brand",
        ]
        self.ad_fatigue_threshold = context.get("ad_fatigue_threshold", 2)
        self.ad_fatigue_active_locations: list = context.get(
            "ad_fatigue_active_locations", []
        )
        self.trigger_record_limit: int = context.get("trigger_record_limit", 5)
        self.fragments_record_limit: int = context.get(
            "fragment_record_limit", 20
        )
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
            "Audience",
            "IsPremium",
        ]

        ## Load payload formatting class
        self.payload = NextAdsPayloadGenerator(
            self.ad_fatigue_active_locations,
            self.ad_fatigue_threshold,
            self.trigger_record_limit,
            self.fragments_record_limit,
        )

        self.lakebaseconnector = NextAdsLakeBaseConnector()

    def predict(self, model_input: dict) -> dict:
        """Generate model prediction for a given input customer RPID & items

        Parameters
        ---------
        model_input:dict
            The input features for predicting on
            rpid: string
                The customers RPID
            items: dict
                Dictionary of latest actions in teh format:
                    {1:{"item": "a", "action" : "view|atb"},
                    {2:{"item": "a", "action" : "view|atb"},}
            Valid actions for input are "view" | "atb"

        Returns:
        -------
        dict
            Output formatted in dictionary structure for bloomreach payload
            #TODO add output format in here
        """
        with ThreadPoolExecutor(max_workers=2) as executor:
            ads = executor.submit(
                self.advert_data_formatting,
                model_input.get("rpid", ""),
                self.pagetype_filter,
            )
            items = executor.submit(
                self.item_data_formatting,
                model_input.get("items", {}),
            )

        ad_df = ads.result()
        item_df = items.result()

        if ad_df.empty | item_df.empty:
            logger.error("No data identified for items or rpid")
            return {}

        cross_data = self.item_customer_weighting_cross(ad_df, item_df)
        if cross_data.empty:
            logger.error("No cross data")
            return {}

        best_ad_df = cross_data[cross_data["AdjustedRanking"] == 1]
        results = self.incrementality(best_ad_df)

        if results.empty:
            logger.error("No incrementality results found")
            return {}

        if not self._has_minimum_ads(results):
            logger.info("Insufficient records for PageTypes")
            return {}

        customer_cells = ad_df[self.customer_cells_columns].drop_duplicates()

        payload = self.payload.build_payload_structure(customer_cells, results)

        if not payload:
            logger.error("No payload generated")
            return {}

        # TODO write to API

        return payload

    ##Reranking Model Functions

    def _has_minimum_ads(self, results: pd.DataFrame) -> bool:
        adverts_per_page = results.groupby("PageType")["UniqueAdID"].count()
        return all(
            adverts_per_page.get(page_type, 0) >= self.min_number_of_ads
            for page_type in self.pagetype_filter
        )

    def advert_data_formatting(
        self, input_rpid: int, page_type_filter: list
    ) -> pd.DataFrame:
        """Retrieve and format the adverts data for the given customer RPID for
        the specified advert page types

        Parameters
        ---------
        input_rpid: int
            The customer RPID number
        page_type_filter: list
            A list of advert page types that are valid for updating

        Returns:
        -------
        pd.DataFrame
            Dataframe of the current scores for the customer adverts
        """
        customer_ads = pd.DataFrame()

        if not input_rpid:
            logger.info("No input RPID provided")
            return customer_ads

        table = f"{self.table_catalog}.{self.table_schema}.next_uk_nextads_realtime_reranking_preranked_ads_online"

        ads_qry = f"""
            SELECT
                *
            FROM {table}
            WHERE
                roamingprofileid={input_rpid}
            ;
            """
        preranked_ads = self.lakebaseconnector.run_query(ads_qry)
        preranked_ads_df = pd.DataFrame(preranked_ads)
        customer_ads = preranked_ads_df[
            preranked_ads_df["PageType"].isin(page_type_filter)
        ]

        if customer_ads.empty:
            logger.error("No current ads found for location")

        return customer_ads

    def item_data_formatting(self, input_features: dict) -> pd.DataFrame:
        """Retrieve and format the items data for the given items

        Parameters
        ---------
        input_features: dict
            dictionary of dictionaries of the item & action
            action can only be 'view' or 'atb'

        Returns:
        -------
        pd.DataFrame
            Dataframe of the item features and weighting factors
        """
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
        products_data = self.lakebaseconnector.run_query(items_qry)

        items_data = products_data.merge(
            input_data, on=["pid", "action"], how="inner"
        ).drop_duplicates()

        if items_data.empty:
            logger.error("Items not found in dataset")

        return items_data

    def item_customer_weighting_cross(
        self, customer_ads: pd.DataFrame, item_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Recalculates the adjusted weighting score for each advert
        through combining the customer and advert features

        Parameters
        ---------
        customer_ads: pd.DataFrame
            Dataframe of the current customer advert scores and features
        item_df: pd.DataFrame
            Dataframe of the recent interaction items, features and weighting

        Returns:
        ------
        pd.Dataframe
            Output dataframe of the customer adverts with an adjusted score
            calculated from the item feature weightings
        """
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
        """Retrieves the 'Next Best Advert' profile based on  top advert most associated to customer behaviour right now
        'Next Best Advert' is determined by advert:advert affinity from item views: item add to basket behaviour

        Parameters
        ---------
        best_ad_df: pd.DataFrame
            Dataframe of the top adverts from the adjusted ranking for the customer

        Returns:
        -------
        pd.DataFrame
            Dataframe of the 'Next Best Advert' profiles based on the best match to current advert
        """
        cust_affinity = pd.DataFrame()

        table = f"{self.table_catalog}.{self.table_schema}.next_uk_nextads_advert_advert_association_online"

        best_ads = "', '".join(best_ad_df["UniqueAdID"].unique())

        ad_affinity_qry = f"""SELECT
                            *
                            FROM {table}
                            WHERE "ViewUniqueAdID" IN ('{best_ads}')
                        ;"""

        ad_affinity_df = self.lakebaseconnector.run_query(ad_affinity_qry)
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
                "AtbCMSPageID",
                "lift_adjusted",
                "AdjustedTriggerScore",
            ]
        ]

        return cust_affinity

    def write_to_bloomreach(self):
        # Multiprocess the steps here!!!
        # TODO Add in additional step here to write out to bloommreach API
        pass
