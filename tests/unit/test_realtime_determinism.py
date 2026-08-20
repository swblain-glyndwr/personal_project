import ast
from pathlib import Path

from next_ads.realtime import unknown as realtime_unknown


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _read(path: str) -> str:
    return (PROJECT_ROOT / path).read_text()


def _compact(source: str) -> str:
    return "".join(source.split())


def _called_names(source: str) -> set[str]:
    names = set()
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Name):
            names.add(node.func.id)
        elif isinstance(node.func, ast.Attribute):
            names.add(node.func.attr)
    return names


def test_viewed_bought_uses_total_orders_and_fails_on_duplicate_keys():
    source = _read("jobs/realtime/viewed_bought.py")
    compact = _compact(source)

    revenue_tie_order = (
        'F.col("total_spend").desc(),F.col("itemno").asc()'
    )
    assert compact.count(revenue_tie_order) >= 2
    assert (
        'Window.partitionBy("itemno1").orderBy('
        'F.col("lift_adjusted").desc(),F.col("itemno2").asc(),)'
        in compact
    )
    assert (
        'F.col("freq1").desc(),F.col("lift_adjusted").desc(),'
        'F.col("itemno1").asc(),F.col("itemno2").asc()'
        in compact
    )

    assert (
        "run_date=capture_run_date(spark)"
        "results=with_run_date(results,run_date)"
        "replace_validated_snapshot("
        "spark,results,table=VB_TABLE_LATEST,key_columns=pk_cols,"
        "columns=results.columns,)"
        in compact
    )
    ddl = _read("sql/realtime/create_table_viewed_bought_latest.sql")
    assert "rundate date not null" in ddl

    called_names = _called_names(source)
    assert "truncate_and_load" not in called_names
    assert "delete_from_and_load" not in called_names
    assert "dropDuplicates" not in called_names
    assert "drop_duplicates" not in called_names


def test_advert_affinity_sorts_item_arrays_and_totally_orders_ties():
    source = _read(
        "src/next_ads/realtime/decisioning/advert_affinity_data_build.py"
    )
    compact = _compact(source)

    assert (
        'F.sort_array(F.collect_set("itemno")).alias("items_list")'
        in compact
    )
    assert (
        'Window.partitionBy("ViewUniqueAdID").orderBy('
        'F.desc(F.col("lift_adjusted")),'
        '*stable_order(["ViewUniqueAdID","AtbUniqueAdID"],'
        'namespace="realtime-ad-affinity-tie",),)'
        in compact
    )


def test_realtime_reporting_totally_orders_latest_action_ties():
    source = _read("jobs/nextads_reporting/realtime_results.py")
    compact = _compact(source)

    assert (
        'Window.partitionBy("AnonRPID").orderBy('
        'F.col("UpdateTimestampDatetime").desc_nulls_last(),'
        'F.col("ResponseTimestamp").desc_nulls_last(),'
        in compact
    )
    assert (
        'Window.partitionBy("RPID").orderBy('
        'F.col("ActionTimestamp").desc(),'
        in compact
    )

    stable_order_calls = [
        node
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "stable_order"
    ]
    stable_orders = {
        next(
            ast.literal_eval(keyword.value)
            for keyword in call.keywords
            if keyword.arg == "namespace"
        ): ast.literal_eval(call.args[0])
        for call in stable_order_calls
    }
    assert stable_orders == {
        "realtime-latest-masid-update-tie": [
            "AnonRPID",
            "ID",
            "UpdateTimestampUnix",
            "MASID",
        ],
        "realtime-latest-action-tie": [
            "RPID",
            "UniqueVisitID",
            "ActionTimestamp",
            "PagePath",
            "ScreenName",
            "Action",
        ],
    }


def test_unknown_backfill_has_stable_personalised_and_default_order(
    monkeypatch,
):
    monkeypatch.setattr(
        realtime_unknown.F,
        "udf",
        lambda function, _return_type: function,
    )
    backfill = realtime_unknown.create_backfill_udf(
        {
            "HP": "HP_default",
            "OC": "OC_default",
            "SB": "SB_default",
        },
        ["HP", "OC", "SB"],
    )

    assert backfill(
        {
            "SB": "SB_personalised",
            "HP": "HP_personalised",
        }
    ) == [
        "HP_personalised",
        "SB_personalised",
        "OC_default",
    ]


def test_unknown_totally_orders_best_ad_map_and_output():
    source = _read("src/next_ads/realtime/unknown.py")
    compact = _compact(source)

    assert "forlocationinsorted(personalized_map)" in compact
    assert "all_locations=sorted(" in compact
    assert (
        'df_top_performing_ads.select("Location","MASIDToken")'
        '.orderBy("Location","MASIDToken").collect()'
        in compact
    )
    assert (
        'F.max(F.struct(F.col("adRelevanceScore"),'
        'F.col("UniqueAdID"),F.col("MASID"),)).alias("_best_ad")'
        in compact
    )
    assert (
        'F.map_from_entries(F.sort_array('
        'F.collect_list(F.struct("Location","best_masid"))))'
        in compact
    )
    assert (
        'F.concat_ws("|",F.col("final_masids"))'
        in compact
    )

    called_names = _called_names(source)
    assert "max_by" not in called_names
    assert "map_from_arrays" not in called_names
