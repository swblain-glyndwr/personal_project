import os
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DateType,
)
from pyspark import pipelines as dp
from pyspark.sql import functions as F
import requests
from pyspark.sql.functions import col, udf
from pyspark.sql.types import ArrayType, MapType
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timezone
from next_ads.utils import config_manager

JOB_ENV = spark.conf.get("pipeline.job_env", "dev")
CLIENT = spark.conf.get("pipeline.client", "next_uk")
LOG_LEVEL = spark.conf.get("pipeline.log_level", "INFO")
TABLE_PREFIX = spark.conf.get("pipeline.table_prefix", "nextads")
USER_SCHEMA = spark.conf.get("pipeline.user_schema", "")

if USER_SCHEMA:
    os.environ["USER_SCHEMA"] = USER_SCHEMA

config = config_manager.load_config(JOB_ENV)

nextschema = ArrayType(
    ArrayType(StringType(), containsNull=False), containsNull=False
)


def call_next_api_fn(api_endpoint, url):
    querystring = {
        "ShowSearchProviderRequestUrl": "true",
        "Criteria": url,
        "Type": "Category",
    }

    # spoof browser headers so it gets past the WAF
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }
    try:
        resp = requests.request(
            "GET", api_endpoint, params=querystring, headers=headers
        )
        response = resp.json()
        status = resp.status_code
    except Exception as e:
        # If request fails, return empty struct with error in request_type
        return [
            ("account_id", ""),
            ("domain_key", ""),
            ("auth_key", ""),
            ("search_type", ""),
            ("view_id", ""),
            ("fl", ""),
            ("request_id", 0),
            ("request_type", str(e)),
            ("start", ""),
            ("rows", ""),
            ("url", ""),
            ("ref_url", ""),
            ("q", ""),
            ("efq", ""),
            ("stats.field", ""),
            ("facet.range", ""),
            ("fq", ""),
            ("filter", ""),
        ]

    if status != 200:
        # Return empty struct, but put response text in request_type
        return [
            ("account_id", ""),
            ("domain_key", ""),
            ("auth_key", ""),
            ("search_type", ""),
            ("view_id", ""),
            ("fl", ""),
            ("request_id", 0),
            ("request_type", resp.text),
            ("start", ""),
            ("rows", ""),
            ("url", ""),
            ("ref_url", ""),
            ("q", ""),
            ("efq", ""),
            ("stats.field", ""),
            ("facet.range", ""),
            ("fq", ""),
            ("filter", ""),
        ]

    # get the searchProviderRequestUrl to be used with bloomreach from the response
    searchProviderRequestUrl = response.get("searchProviderRequestUrl", "")
    searchProviderRequestUrlp = parse_qs(searchProviderRequestUrl)

    # flatten the values in the requesturl
    ha = {
        k: v[0] if k != "fq" else v
        for k, v in searchProviderRequestUrlp.items()
    }

    # remove the url in the response
    ha.pop(
        config.next_search_wrapper,
        None,
    )

    ha["fl"] = "pid"  # we only need the pid
    ha.pop("start")  # we will handle pagination separately
    ha.pop("rows")  # we will handle pagination separately
    ha["account_id"] = 6042

    params_list1 = []
    params_list2 = []

    # there can be many fq values in a single search api call, we need them all
    if "fq" in ha:
        params_list1 = [("fq", v) for v in ha["fq"]]

    params_list2 = []

    # these should all be a single value
    for k, v in ha.items():
        if k != "fq":
            params_list2.append((k, v))

    # concat all the data together
    if params_list1:
        result = params_list2 + params_list1
    else:
        result = params_list2
    return result


@udf(returnType=nextschema)
def call_next_api(api_endpoint, url):
    """Args:
        url (str): The category url we want a Bloomreach API query for

    Returns:
        dict: a dict object that can be used later to query the BR API for the items returned from this category URL
    """
    result = call_next_api_fn(api_endpoint, url)
    return result


def call_br_api_fn(url, params_array, rows_per_page=84, max_pages=3):
    all_values = []
    start = 0
    page_count = 0

    params = [tuple(pair) for pair in params_array]

    while True:
        # append on the rows settings
        params.append(("start", start))
        params.append(("rows", rows_per_page))

        # make the calls
        response = requests.get(url, params=params)

        # remove the rows settings again
        del params[-2:]

        if response:
            data = response.json()
            docs = data.get("response", {}).get("docs", [])
            num_found = data.get("response", {}).get("numFound", 0)

            # Collect only the values from each doc (item)
            for doc in docs:
                all_values.extend(doc.values())

            # Pagination logic
            page_count += 1
            start += rows_per_page
            if start >= num_found:
                break
            if max_pages is not None and page_count >= max_pages:
                break
        else:
            all_values.append(response.text)
            break

    return all_values


@udf(ArrayType(StringType()))
def call_br_api(url, params_array, rows_per_page=84, max_pages=3):
    """Args:
        url (str): The API endpoint.
        params (dict): Query parameters.
        rows_per_page (int): Number of rows per page.
        max_pages (int or None): Maximum number of pages to fetch (None = all pages).

    Returns:
        list: A flat list of all items from docs across pages.
    """
    result = call_br_api_fn(url, params_array, rows_per_page, max_pages)
    return result


# Columns
def format_url(url):
    col = F.when(F.col(url).contains("www.next."), F.col(url)).otherwise(
        F.when(
            F.col(url).startswith("\\"),
            F.substring(F.col(url), 2, 10000),
        ).otherwise(
            F.concat(
                F.lit("https://www.next.co.uk/"),
                F.substring(F.col(url), 2, 10000),
            )
        )
    )
    return col


def url_type(url):
    col = (
        F.when(F.col(url).contains("promotion"), F.lit("promotion"))
        .when(F.col(url).contains("search?w"), F.lit("keyword"))
        .otherwise(F.lit("category"))
    )
    return col


def lookup_key(search_type):
    col = F.when(
        F.col(search_type) == "promotion", F.lit("promotion")
    ).otherwise(F.lit("search?w"))
    return col


# Define UDFs
def parse_url_udf(url):
    parsed = urlparse(url)
    if parsed.query:
        # parse_qs returns lists, convert to single values for convenience
        return {
            k: v[0] if len(v) == 1 else v
            for k, v in parse_qs(parsed.query).items()
        }
    else:
        return parsed.path.strip("/").split("/")


# The return type is tricky because we return either dict or list
# We'll cast everything as a MapType(StringType, StringType) for query strings,
# and convert lists to Map with keys as indices for paths
def parse_url_struct(url):
    parsed = urlparse(url)
    if parsed.query:
        return {
            k: v[0] if len(v) == 1 else ",".join(v)
            for k, v in parse_qs(parsed.query).items()
        }
    else:
        return {
            str(i): seg
            for i, seg in enumerate(parsed.path.strip("/").split("/"))
        }


parse_udf = udf(parse_url_struct, MapType(StringType(), StringType()))


def parse_and_prep_data(data):
    """Parse and prepare URL data for search API queries.
    This function processes a DataFrame containing URL data by:
    1. Parsing URLs using a UDF to extract query parameters
    2. Splitting parsed URL components into key-value pairs
    3. Merging key-value pairs from multiple URL sections into a single map
    4. Extracting promotion identifiers from the URL map
    5. Constructing a formatted query structure compatible with search API requests
    Args:
        data (pyspark.sql.DataFrame): Input DataFrame containing at minimum:
            - url (str): The URL to parse
            - url_type (str): Classification of the URL type
            - MASIDtoken (str): MASID token identifier
    Returns:
        pyspark.sql.DataFrame: Transformed DataFrame with columns:
            - UniqueAdID (str): Unique advertisement identifier
            - URL (str): Original URL
            - url_type (str): URL type classification
            - MASIDtoken (str): MASID token
            - br_url (str): bloomreach search provider URL
            - next_url (str): Next API search endpoint
            - query_struct (array): Array of key-value pairs formatted for API requests
    """
    data = (
        data.withColumn("url_type", url_type("url"))
        .withColumn("lookup_key", lookup_key("url_type"))
        .withColumnRenamed("url", "oldurl")
        .withColumn("url", format_url("oldurl"))
        .drop("oldurl")
    )

    df_parsed = (
        data.withColumn("parsed", parse_udf(col("url")))
        .withColumn("querystring", F.col("parsed").getItem("w"))
        .withColumn("parts1", F.split(F.col("parsed").getItem(1), "-"))
        .withColumn("parts2", F.split(F.col("parsed").getItem(2), "-"))
        .withColumn(  # first section of url
            "parts_kv1",
            F.expr("""
            IF(size(parts1) >= 2,
                map_from_arrays(
                    transform(sequence(0, CAST(size(parts1) / 2 AS INT) - 1), x -> parts1[x * 2]),
                    transform(sequence(0, CAST(size(parts1) / 2 AS INT) - 1), x -> parts1[x * 2 + 1])
                ),
                map_from_arrays(CAST(array() AS ARRAY<STRING>), CAST(array() AS ARRAY<STRING>))
            )
        """),
        )
        .withColumn(  # second section of url
            "parts_kv2",
            F.expr("""
        IF(size(parts2) >= 2,
            map_from_arrays(
                transform(sequence(0, CAST(size(parts2) / 2 AS INT) - 1), x -> parts2[x * 2]),
                transform(sequence(0, CAST(size(parts2) / 2 AS INT) - 1), x -> parts2[x * 2 + 1])
            ),
            map_from_arrays(CAST(array() AS ARRAY<STRING>), CAST(array() AS ARRAY<STRING>))
        )
    """),
        )
        .withColumn(  # get all the key value pairs from the url into a single map
            "parts_kv_s", F.map_concat(F.col("parts_kv1"), F.col("parts_kv2"))
        )
        .withColumn(  # extract the promotion from where it is stored in the url
            "promotion",
            F.expr(
                """
            CASE
            WHEN map_contains_key(parts_kv_s, 'promotion')
            THEN parts_kv_s['promotion']
            ELSE NULL
            END
        """
            ),
        )
        .drop("parts_kv_1, parts_kv2")
    )

    # Convert fq struct/map to array of arrays: [["fq", "key:\"escaped_value\""], ...]
    fq_array_expr = F.expr("""
        transform(
            map_entries(fq),
            x -> array(
                'fq',
                concat(
                    x.key,
                    ':',
                    '"',
                    regexp_replace(x.value, '"', '\\\\"'),
                    '"'
                )
            )
            )
        """)

    now = datetime.now(timezone.utc)
    request_id = now.strftime("%Y%m%d%H%M%S%f") + "0"

    # build a query array here for making keyword/search queries
    # as the category queries are constructed by the next api for us
    df_qry = (
        df_parsed.selectExpr(
            "UniqueAdID",
            "url as URL",
            "url_type",
            "MASIDtoken",
            "promotion as q",  # use the name of the promotion as the query for search type queries
            f"'{config.next_search_wrapper}' as br_url",
            f"'{config.next_search_endpoint}' as next_url",
            "'6042' as account_id",
            "'' as auth_key",
            "'next' as domain_key",
            "'keyword' as search_type",
            "'gb' as view_id",
            "'pid' as fl",
            f"{request_id}::int as request_id",
            "'search' as request_type",
            "2 as rows",
            "0 as start",
            "'www.next.co.uk?_br_var_1=true' as ref_url",
            "'' as efq",
            "'sale_price' as `stats.field`",
            "'sale_price' as `facet.range`",
            "parts_kv_s as fq",
            "'-issale:gbs' as filter",
        )
        .withColumn("fq_array", fq_array_expr)
        .withColumn("first_fq", F.col("fq_array").getItem(0).getItem(1))
        .selectExpr(
            "UniqueAdID",
            "URL",
            "url_type",
            "MASIDtoken",
            "br_url",
            "next_url",
            # map to an array of arrays as this is what can be used with an API request later
            "array(array('account_id', account_id), array('auth_key',auth_key), array('domain_key', domain_key), array('search_type', search_type), array('request_type', request_type), array('ref_url', ref_url ), array('url', URL ), array('view_id', view_id), array('fl', fl),array('q', q), array('efq', efq), array('stats.field', `stats.field`), array('facet.range', `facet.range`), array('filter', filter))  as query_struct",
        )
        .drop("fq_array", "fq")
    )
    return df_qry


@dp.view(name="control_sheet")
def control_sheet():
    s_control_sheet = spark.table(
        config.tables_write.control_sheet_raw_latest_v2
    )
    return s_control_sheet


@dp.materialized_view(
    name="query_prep",
    comment="Querying the next API for the category URLs to get the searchProviderRequestUrl and other details needed to query the Bloomreach API",
    private=True,
)
def query_prep():
    s_control_sheet = spark.table("control_sheet")

    data = (
        s_control_sheet.filter(
            "Status = 'Active' AND MASIDToken IS NOT NULL AND URL IS NOT NULL AND URL != 'TBC'"
        )
        .select("UniqueAdID", "URL", "MASIDToken")
        .distinct()
    )

    df_qry = parse_and_prep_data(data)

    df_via_next_qry = df_qry.filter("url_type = 'category'")
    df_direct_qry = df_qry.filter("url_type <> 'category'")

    df_via_next_qry_queried = df_via_next_qry.withColumn(
        "searchProviderRequest", call_next_api(F.col("next_url"), F.col("url"))
    )
    df_via_next_qry_queried = df_via_next_qry_queried.drop(
        "query_struct"
    ).withColumnRenamed("searchProviderRequest", "query_struct")

    BR_ready = df_direct_qry.unionByName(df_via_next_qry_queried)

    return BR_ready


@dp.materialized_view(
    name="full_output",
    comment="Querying Bloomreach API for each URL",
    private=True,
)
def full_output():
    BR_ready = spark.table("query_prep")

    df_queried = BR_ready.repartition(10).withColumn(
        "queryresponse", call_br_api(F.col("br_url"), F.col("query_struct"))
    )

    p_result = df_queried.select(
        "UniqueAdID",
        "URL",
        "MASIDtoken",
        F.posexplode("queryresponse").alias("raw_pos", "items"),
    ).selectExpr(
        "UniqueAdID",
        "URL",
        "MASIDtoken",
        "raw_pos + 1 as item_pos",
        "items as item",
    )

    return p_result


sort_order_schema = StructType(
    [
        StructField("UniqueAdID", StringType(), True),
        StructField("URL", StringType(), True),
        StructField("MASIDtoken", StringType(), True),
        StructField("item_pos", IntegerType(), True),
        StructField("item", StringType(), True),
        StructField("UniqueAdIDPremium", StringType(), True),
        StructField("CMSPageID", StringType(), True),
        StructField("PotNumber", StringType(), True),
        StructField("CampaignNumber", StringType(), True),
        StructField("Title", StringType(), True),
        StructField("AlgoDivision", StringType(), True),
        StructField("TradeDivision", StringType(), True),
        StructField("Brand", StringType(), True),
        StructField("ProductListingPage", StringType(), True),
        StructField("ForYouPage", StringType(), True),
        StructField("CheckoutPage", StringType(), True),
        StructField("ShoppingBagPage", StringType(), True),
        StructField("HomePage", StringType(), True),
        StructField("Segment", StringType(), True),
        StructField("AdDriver", StringType(), True),
        StructField("TemplateName", StringType(), True),
        StructField("StartDate", StringType(), True),
        StructField("EndDate", StringType(), True),
        StructField("Status", StringType(), True),
        StructField("AudienceOnly", StringType(), True),
        StructField("Items", StringType(), True),
        StructField("Tags", StringType(), True),
        StructField("Themes", StringType(), True),
        StructField("AdVariant", StringType(), True),
        StructField("rundate", DateType(), True),
    ]
)


@dp.materialized_view(
    name=config.tables_write.sort_order_v2_latest,
    comment="Join back on the control sheet and publish",
    schema=sort_order_schema,
)
def sort_order_latest():
    p_result = spark.table("full_output")
    s_control_sheet = spark.table("control_sheet")

    output = p_result.join(
        s_control_sheet.drop("URL", "MASIDToken"),
        on=["UniqueAdID"],
        how="inner",
    )
    output = output.orderBy("UniqueAdID", "item_pos")

    return output
