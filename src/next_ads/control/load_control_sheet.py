"""Reusable control sheet loading helpers."""

import json
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Mapping, Sequence

import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from dsutils.etl import assert_pk

from next_ads.ranking.scoring import append_targeting_criteria
from next_ads.data.validation import schemas


@dataclass(frozen=True)
class ControlSheetLocations:
    """Resolved control sheet location groups."""

    valid_locations: list[str]
    read_locations: list[str]
    inherited_locations: dict[str, str]


@dataclass(frozen=True)
class ControlSheetOutputTables:
    """Resolved control sheet output table route for a job run."""

    catalog_write: str
    schema_write: str
    control_sheet: str
    control_sheet_latest: str
    control_sheet_raw: str
    control_sheet_raw_latest: str
    control_sheet_plp_raw: str
    control_sheet_plp_raw_latest: str
    multipage_locations: str
    multipage_locations_latest: str


@dataclass(frozen=True)
class ControlSheetRunContext:
    """Resolved control sheet run configuration."""

    client: str
    locations: Mapping[str, Mapping[str, object]]
    location_config: ControlSheetLocations
    control_sheet: Mapping[str, object]
    placements_sheet: Mapping[str, object]
    plx_urls_sheet: Mapping[str, object]
    output_tables: ControlSheetOutputTables
    schema_write: str
    target_table: str
    target_table_latest: str
    target_multipage_locations_table: str
    target_multipage_locations_latest_table: str
    webhook_url: str
    control_sheet_read_schema: list[list[str]]
    date_format: str
    date_regex: str


@dataclass(frozen=True)
class TargetColumnAlignment:
    """Control sheet DataFrame aligned to a target table schema."""

    df: DataFrame
    extra_columns: list[str]


@dataclass(frozen=True)
class DuplicateMasidResolution:
    """Result from legacy duplicate MASID conflict handling."""

    df: DataFrame
    warning_message: str | None
    duplicate_masids: list[str]


@dataclass(frozen=True)
class ControlSheetValidationResult:
    """Soft validation output for control-sheet inputs."""

    df_control_sheet: DataFrame
    df_placements: DataFrame
    df_plx_urls: DataFrame
    errors_json_by_input: dict[str, str]


@dataclass(frozen=True)
class ProcessedControlSheet:
    """Processed control-sheet output and evidence details."""

    df: DataFrame
    invalid_date_ad_ids: list[str]
    active_ad_count: int
    active_locations: list[str]
    active_ad_location_count: int
    duplicate_masid_resolution: DuplicateMasidResolution
    target_alignment: TargetColumnAlignment


def resolve_control_sheet_locations(
    locations: Mapping[str, Mapping[str, object]],
) -> ControlSheetLocations:
    """Split configured locations into directly read and inherited groups."""
    valid_locations = list(locations.keys())
    read_locations = []
    inherited_locations = {}

    for location, settings in locations.items():
        inherit_ads_from = settings.get("inherit_ads_from")
        if inherit_ads_from:
            inherited_locations[location] = str(inherit_ads_from)
        else:
            read_locations.append(location)

    return ControlSheetLocations(
        valid_locations=valid_locations,
        read_locations=read_locations,
        inherited_locations=inherited_locations,
    )


def build_control_sheet_run_context(
    *,
    client: str,
    client_config: Mapping[str, object],
    config,
) -> ControlSheetRunContext:
    """Resolve control-sheet configuration for a job run."""
    locations = client_config["locations"]
    location_config = resolve_control_sheet_locations(locations)
    control_sheet = client_config["control_sheet"]
    output_tables = resolve_control_sheet_output_tables(config)
    control_sheet_read_schema = build_control_sheet_read_schema(
        control_sheet["read_schema"],
        location_config.read_locations,
    )

    return ControlSheetRunContext(
        client=client,
        locations=locations,
        location_config=location_config,
        control_sheet=control_sheet,
        placements_sheet=client_config["placements_sheet"],
        plx_urls_sheet=client_config["plx_urls_sheet"],
        output_tables=output_tables,
        schema_write=output_tables.schema_write,
        target_table=output_tables.control_sheet,
        target_table_latest=output_tables.control_sheet_latest,
        target_multipage_locations_table=output_tables.multipage_locations,
        target_multipage_locations_latest_table=(
            output_tables.multipage_locations_latest
        ),
        webhook_url=client_config["webhooks"]["Input Warnings"],
        control_sheet_read_schema=control_sheet_read_schema,
        date_format=control_sheet["date_format"],
        date_regex=control_sheet["date_regex"],
    )


def build_control_sheet_read_schema(
    base_schema: Sequence[Sequence[str]],
    read_locations: Sequence[str],
) -> list[list[str]]:
    """Return the configured control sheet schema with location columns."""
    schema = [list(column) for column in base_schema]
    existing_columns = {column[0] for column in schema}

    for location in read_locations:
        if location not in existing_columns:
            schema.append([location, "string", "null"])

    return schema


def align_control_sheet_to_read_schema(
    df_control_sheet: DataFrame,
    read_schema: Sequence[Sequence[str]],
) -> TargetColumnAlignment:
    """Keep the configured positional sheet columns and drop surplus headers."""
    expected_columns = [column[0] for column in read_schema]
    actual_columns = list(df_control_sheet.columns)

    if len(actual_columns) < len(expected_columns):
        missing_count = len(expected_columns) - len(actual_columns)
        raise ValueError(
            "Control Sheet has fewer columns than the configured read schema "
            f"({len(actual_columns)} found, {len(expected_columns)} expected; "
            f"{missing_count} missing)."
        )

    if len(actual_columns) == len(expected_columns):
        return TargetColumnAlignment(df=df_control_sheet, extra_columns=[])

    temp_columns = [
        f"__control_sheet_column_{index}" for index in range(len(actual_columns))
    ]
    df_with_unique_columns = df_control_sheet.toDF(*temp_columns)
    df_aligned = df_with_unique_columns.select(
        *[
            F.col(temp_columns[index]).alias(column_name)
            for index, column_name in enumerate(expected_columns)
        ]
    )

    return TargetColumnAlignment(
        df=df_aligned,
        extra_columns=actual_columns[len(expected_columns) :],
    )


def assert_append_rundate_target_schema(
    *,
    table_name: str,
    df_columns: Sequence[str],
    target_columns: Sequence[str],
) -> None:
    """Validate target table columns for dsutils append-rundate loaders."""
    expected_columns = [*df_columns, "rundate"]
    actual_columns = list(target_columns)

    if actual_columns == expected_columns:
        return

    missing_columns = [
        column for column in expected_columns if column not in actual_columns
    ]
    extra_columns = [
        column for column in actual_columns if column not in expected_columns
    ]
    first_order_mismatch = next(
        (
            f"position {index}: expected {expected}, found {actual}"
            for index, (expected, actual) in enumerate(
                zip(expected_columns, actual_columns)
            )
            if expected != actual
        ),
        None,
    )

    details = [
        f"Target table {table_name} does not match the DataFrame load schema.",
        "delete_from_and_load/truncate_and_load insert by position and append "
        "`current_date() as rundate`, so table columns must match exactly.",
    ]
    if missing_columns:
        details.append(f"Missing target columns: {', '.join(missing_columns)}.")
    if extra_columns:
        details.append(f"Unexpected target columns: {', '.join(extra_columns)}.")
    if first_order_mismatch:
        details.append(f"First order mismatch: {first_order_mismatch}.")

    raise ValueError(" ".join(details))


def resolve_control_sheet_output_tables(config) -> ControlSheetOutputTables:
    """Resolve environment-specific control sheet output tables."""
    tables_write = config.tables_write
    return ControlSheetOutputTables(
        catalog_write=config.catalog_write,
        schema_write=config.schema_write,
        control_sheet=tables_write.control_sheet,
        control_sheet_latest=tables_write.control_sheet_latest,
        control_sheet_raw=tables_write.control_sheet_raw,
        control_sheet_raw_latest=tables_write.control_sheet_raw_latest,
        control_sheet_plp_raw=tables_write.control_sheet_plp_raw,
        control_sheet_plp_raw_latest=tables_write.control_sheet_plp_raw_latest,
        multipage_locations=tables_write.multipage_locations,
        multipage_locations_latest=tables_write.multipage_locations_latest,
    )


def build_multipage_locations(df_plx_urls: DataFrame) -> DataFrame:
    """Transform PLX URL rows into multipage location records."""
    return (
        df_plx_urls.withColumnRenamed("URL", "Page")
        .withColumn("Screen", F.lit("PLP"))
        .withColumn("Location", F.lit("PLX"))
        .select("Location", "Page", "Screen")
        .drop_duplicates()
    )


def validate_control_sheet_inputs(
    *,
    df_control_sheet: DataFrame,
    df_placements: DataFrame,
    df_plx_urls: DataFrame,
) -> ControlSheetValidationResult:
    """Run legacy soft validation for control-sheet inputs."""
    df_control_sheet = filter_non_empty_unique_ads(df_control_sheet).filter(
        df_control_sheet.CMSPageID != ""
    )
    df_control_sheet = schemas.ControlSheetInputModel.validate(
        df_control_sheet,
        lazy=True,
    )
    control_sheet_errors_json = json.dumps(
        dict(df_control_sheet.pandera.errors),
        indent=2,
    )

    df_placements = schemas.ControlSheetPlacementsInputModel.validate(
        df_placements,
        lazy=True,
    )
    placements_errors_json = json.dumps(
        dict(df_placements.pandera.errors),
        indent=2,
    )

    df_plx_urls = schemas.ControlSheetPLXInputModel.validate(
        df_plx_urls,
        lazy=True,
    )
    plx_urls_errors_json = json.dumps(
        dict(df_plx_urls.pandera.errors),
        indent=2,
    )

    return ControlSheetValidationResult(
        df_control_sheet=df_control_sheet,
        df_placements=df_placements,
        df_plx_urls=df_plx_urls,
        errors_json_by_input={
            "control_sheet": control_sheet_errors_json,
            "placements": placements_errors_json,
            "plx_urls": plx_urls_errors_json,
        },
    )


def filter_non_empty_unique_ads(df_control_sheet: DataFrame) -> DataFrame:
    """Remove blank control-sheet ad rows."""
    return df_control_sheet.where(F.col("UniqueAdID") != "")


def filter_valid_date_format(
    df_control_sheet: DataFrame,
    date_regex: str,
) -> DataFrame:
    """Keep control-sheet rows with parseable start and end date strings."""
    return df_control_sheet.where(
        (F.col("StartDate").rlike(date_regex))
        & (F.col("EndDate").rlike(date_regex))
    )


def collect_invalid_date_ad_ids(
    df_control_sheet: DataFrame,
    df_valid_date_format: DataFrame,
) -> list[str]:
    """Collect ad IDs removed by the legacy date-format check."""
    df_invalid_date_ads = df_control_sheet.join(
        df_valid_date_format,
        on="UniqueAdID",
        how="leftanti",
    ).select("UniqueAdID")
    return [row[0] for row in df_invalid_date_ads.collect()]


def get_active_control_ads(
    df_valid_date_format: DataFrame,
    date_format: str,
    reference_date: date | None = None,
) -> DataFrame:
    """Return ads active for the legacy tomorrow-based run date."""
    active_date = (reference_date or date.today()) + timedelta(days=1)
    return (
        df_valid_date_format.drop("Status")
        .withColumn("StartDate", F.to_date(F.col("StartDate"), date_format))
        .withColumn("EndDate", F.to_date(F.col("EndDate"), date_format))
        .where(F.col("StartDate") <= active_date)
        .where(F.col("EndDate") >= active_date)
    )


def normalise_active_control_ads(df_control_active: DataFrame) -> DataFrame:
    """Apply legacy active-ad field coercions."""
    return (
        df_control_active.withColumn(
            "Items",
            F.regexp_replace(F.upper(F.col("Items")), "-", ""),
        )
        .withColumn(
            "AudienceOnlyInt",
            F.when(F.col("AudienceOnly") == "TRUE", 1).otherwise(0),
        )
        .drop("AudienceOnly")
        .withColumnRenamed("AudienceOnlyInt", "AudienceOnly")
    )


def apply_inherited_location_columns(
    df_control_active: DataFrame,
    inherited_locations: Mapping[str, str],
) -> DataFrame:
    """Copy configured inherited location flags onto active ads."""
    for location, inherit_from in inherited_locations.items():
        df_control_active = df_control_active.withColumn(
            location,
            F.col(inherit_from),
        )
    return df_control_active


def build_requested_ad_locations(
    df_control_active: DataFrame,
    valid_locations: Sequence[str],
) -> DataFrame:
    """Build active ad-location requests from control-sheet flags."""
    return (
        df_control_active.unpivot(
            ids="UniqueAdID",
            values=list(valid_locations),
            variableColumnName="Location",
            valueColumnName="Requested",
        )
        .where(F.col("Requested") == "TRUE")
        .drop_duplicates()
        .drop("Requested")
    )


def build_processed_control_sheet(
    df_control_active: DataFrame,
    df_placements: DataFrame,
    valid_locations: Sequence[str],
) -> DataFrame:
    """Build the joined control-sheet output before quality checks."""
    df_id_loc = build_requested_ad_locations(df_control_active, valid_locations)
    df_ad_attributes = (
        df_control_active.drop(*list(valid_locations))
        .drop_duplicates()
        .replace("", None)
    )
    df_processed = df_id_loc.join(
        df_ad_attributes,
        on="UniqueAdID",
        how="left",
    )
    df_processed = df_processed.join(df_placements, on="Location", how="left")
    df_processed = df_processed.fillna({"ModelCombination": "and"}).withColumn(
        "ModelCombination",
        F.when(F.col("Models").isNull(), F.lit(None)).otherwise(
            F.col("ModelCombination")
        ),
    )
    return append_targeting_criteria(df_processed)


def constrain_premium_ads_to_sibling_locations(
    df_processed: DataFrame,
) -> DataFrame:
    """Only keep premium sibling ads where the target location is valid."""
    location_lookup_df = (
        df_processed.groupBy("UniqueAdID")
        .agg(F.collect_set("Location").alias("ValidLocations"))
        .withColumnRenamed("UniqueAdID", "LookupAdID")
    )
    df_processed = df_processed.join(
        location_lookup_df,
        df_processed["UniqueAdIDPremium"] == location_lookup_df["LookupAdID"],
        "left",
    )

    return df_processed.withColumn(
        "UniqueAdIDPremium",
        F.when(
            (F.col("UniqueAdIDPremium").isNotNull())
            & (
                F.col("ValidLocations").isNull()
                | ~F.array_contains(F.col("ValidLocations"), F.col("Location"))
            ),
            F.lit(None),
        ).otherwise(F.col("UniqueAdIDPremium")),
    ).drop("LookupAdID", "ValidLocations")


def resolve_duplicate_masid_conflicts(
    df_processed: DataFrame,
) -> DuplicateMasidResolution:
    """Apply legacy duplicate MASID suffix conflict resolution."""
    df_dup_masids = (
        df_processed.groupBy("AlgoDivision", "Location", "MASIDToken")
        .agg(F.countDistinct("UniqueAdID").alias("AdsPerMASID"))
        .where(F.col("AdsPerMASID") > 1)
    )

    if df_dup_masids.count() <= 1:
        return DuplicateMasidResolution(
            df=df_processed,
            warning_message=None,
            duplicate_masids=[],
        )

    dup_masid_list = sorted(
        set([row[0] for row in (df_dup_masids.select("MASIDToken").collect())])
    )

    warning_message = (
        "Duplicate MASID suffixes assigned to Ads"
        + f" in same AlgoDivision: {dup_masid_list}"
    )

    for masid in dup_masid_list:
        resolution_message = f"Resolving conflict for MASID suffix: {masid}"
        warning_message += "\n\n" + resolution_message

        df_dups_masid = (
            df_processed.where(F.col("MASIDToken") == masid)
            .select("UniqueAdID")
            .collect()
        )

        clashing_ids = list(set([row[0] for row in df_dups_masid]))
        clashing_ids.sort()
        try:
            keep_ad = f"Keeping ad: {clashing_ids[-1]}"
            warning_message += "\n" + keep_ad

            ids_to_delete = clashing_ids[:-1]

            for id_to_delete in ids_to_delete:
                drop_ad = f"Dropping conflicting ad: {id_to_delete}"
                warning_message += "\n" + drop_ad
                df_processed = df_processed.where(
                    F.col("UniqueAdID") != id_to_delete
                )
        except IndexError:
            issue_ad = (
                "Issue resolving conflict for ads with MASID suffix:"
                + f" {masid} - all {masid} ads removed"
            )
            warning_message += "\n" + issue_ad
            df_processed = df_processed.where(F.col("MASIDToken") != masid)

    return DuplicateMasidResolution(
        df=df_processed,
        warning_message=warning_message,
        duplicate_masids=dup_masid_list,
    )


def clean_theme_strings(df_processed: DataFrame) -> DataFrame:
    """Apply legacy theme string normalisation."""
    return df_processed.withColumn(
        "Themes",
        F.when(
            F.col("Themes").isNotNull(),
            F.trim(F.lower(F.col("Themes"))),
        ).otherwise(F.col("Themes")),
    )


def clear_missing_premium_ad_ids(df_processed: DataFrame) -> DataFrame:
    """Clear premium ad references that are not present in the output."""
    df_valid_ad_ids = df_processed.select(
        F.col("UniqueAdID").alias("valid_id")
    ).distinct()
    df_processed = df_processed.join(
        df_valid_ad_ids,
        F.col("UniqueAdIDPremium") == F.col("valid_id"),
        "left_outer",
    )
    return df_processed.withColumn(
        "UniqueAdIDPremium",
        F.when(F.col("valid_id").isNull(), F.lit(None)).otherwise(
            F.col("UniqueAdIDPremium")
        ),
    ).drop("valid_id")


def align_control_sheet_to_target_columns(
    df_processed: DataFrame,
    target_cols: Sequence[str],
) -> TargetColumnAlignment:
    """Drop superfluous columns when the target schema is a subset."""
    target_col_set = set(target_cols)
    processed_col_set = set(df_processed.columns)

    if target_col_set == processed_col_set:
        return TargetColumnAlignment(df=df_processed, extra_columns=[])

    if target_col_set.issubset(processed_col_set):
        extra_columns = sorted(processed_col_set.difference(target_col_set))
        return TargetColumnAlignment(
            df=df_processed.drop(*extra_columns),
            extra_columns=extra_columns,
        )

    raise ValueError("Target table cols not a subset of Control Sheet cols")


def process_control_sheet(
    *,
    df_control_sheet: DataFrame,
    df_placements: DataFrame,
    valid_locations: Sequence[str],
    inherited_locations: Mapping[str, str],
    date_format: str,
    date_regex: str,
    target_cols: Sequence[str],
    reference_date: date | None = None,
) -> ProcessedControlSheet:
    """Process control-sheet rows into the output table shape."""
    df_control_not_empty = filter_non_empty_unique_ads(df_control_sheet)
    df_valid_date_format = filter_valid_date_format(
        df_control_not_empty,
        date_regex,
    )
    df_valid_date_format.count()

    invalid_date_ad_ids = collect_invalid_date_ad_ids(
        df_control_not_empty,
        df_valid_date_format,
    )

    df_control_active = get_active_control_ads(
        df_valid_date_format,
        date_format,
        reference_date=reference_date,
    )
    active_ad_count = df_control_active.count()
    df_control_active = normalise_active_control_ads(df_control_active)
    df_control_active = apply_inherited_location_columns(
        df_control_active,
        inherited_locations,
    )

    df_id_loc = build_requested_ad_locations(df_control_active, valid_locations)
    active_locations = sorted(
        set([row[0] for row in df_id_loc.select("Location").collect()])
    )
    active_ad_location_count = df_id_loc.count()

    df_processed = build_processed_control_sheet(
        df_control_active,
        df_placements,
        valid_locations,
    )
    df_processed = constrain_premium_ads_to_sibling_locations(df_processed)
    assert_pk(df_processed, ["UniqueAdID", "Location"])

    duplicate_masid_resolution = resolve_duplicate_masid_conflicts(df_processed)
    df_processed = duplicate_masid_resolution.df

    df_processed = clean_theme_strings(df_processed)
    df_processed = clear_missing_premium_ad_ids(df_processed)

    target_alignment = align_control_sheet_to_target_columns(
        df_processed,
        target_cols,
    )

    return ProcessedControlSheet(
        df=target_alignment.df,
        invalid_date_ad_ids=invalid_date_ad_ids,
        active_ad_count=active_ad_count,
        active_locations=active_locations,
        active_ad_location_count=active_ad_location_count,
        duplicate_masid_resolution=duplicate_masid_resolution,
        target_alignment=target_alignment,
    )
