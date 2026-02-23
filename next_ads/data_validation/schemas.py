import pandera.pyspark as pa
from pandera.pyspark import DataFrameModel
from pyspark.sql.types import StringType

from next_ads.data_validation import custom_checks  # noqa: F401


class ControlSheetInputModel(DataFrameModel):
    """Input schema for Google Sheets control sheet data"""

    _CMSPageID_PATTERN = r"^c[0-9]+(_[a-zA-Z]+[0-9]+)*$"
    _MASID_TOKEN_PATTERN = (
        r"^[A-Z]{4}$"  # Not a hard constraint - Exactly 4 uppercase letters
    )

    UniqueAdID: StringType = pa.Field(
        nullable=False,
        description="Unique ad identifier",
        unique_spark={"check": True},
    )
    Realm: StringType = pa.Field(
        nullable=False,
        isin_spark={"allowed_values": ["Next"]},
    )
    Territory: StringType = pa.Field(
        nullable=False,
        isin_spark={"allowed_values": ["GB"]},
    )
    Status: StringType = pa.Field(
        nullable=True,
        # Status is not used so no need to enforce strict check
        isin_spark={
            "allowed_values": ["active", "inactive", "Active", "Inactive"]
        },
    )
    CMSPageID: StringType = pa.Field(
        nullable=False,
        str_matches_spark={"pattern": _CMSPageID_PATTERN},
        unique_spark={"check": True},
    )
    MASIDToken: StringType = pa.Field(
        nullable=False,
        str_matches_spark={"pattern": _MASID_TOKEN_PATTERN},
        unique_spark={"check": True},
    )

    class Config:
        """ControlSheetInputModel config"""

        name = "control_sheet_input"
        strict = False  # False to Allow additional PL* columns
        coerce = True  # Attempt type coercion


class ControlSheetPlacementsInputModel(DataFrameModel):
    """Input schema for Google Sheets control sheet Placements data"""

    Location: StringType = pa.Field(
        nullable=False,
        unique_spark={"check": True},
    )
    Page: StringType = pa.Field(nullable=False)
    Screen: StringType = pa.Field(nullable=True)

    class Config:
        """ControlSheetInputModel config"""

        name = "control_sheet_placements_input"
        strict = False
        coerce = True


class ControlSheetPLXInputModel(DataFrameModel):
    """Input schema for Google Sheets control sheet PLX data"""

    URL: StringType = pa.Field(
        nullable=False,
        unique_spark={"check": True},
    )

    class Config:
        """ControlSheetInputModel config"""

        name = "control_sheet_plx_input"
        strict = False
        coerce = True


class GlobalSolutionOutputModel(DataFrameModel):
    """Output schema for processed control sheet data"""

    # Example: PLX_POAA-c943|PLX_TAAA-c164_v2
    _MASID_CMS_CONTENT_PATTERN = (
        r"^[A-Za-z0-9_]*-"
        r"[A-Za-z0-9_\.\-]+"
        r"(\|[A-Za-z0-9_]*-[A-Za-z0-9_\.\-]+)*$"
    )

    # URL Path Pattern - RFC 3986 compliant
    # Validates paths starting with / and optionally includes query params and
    # fragments
    # Example: /over/there?name=ferret#nose
    _URL_PATH_PATTERN = (
        r"^/"  # Must start with /
        # Path: URL-safe chars, optional trailing /
        r"[A-Za-z0-9._~!$&'()*+,;=:@%/\-]*" 
        # Query: optional ?key=value&key2=value2
        r"(\?[A-Za-z0-9._~!$&'()*+,;=:@%/\-]*)?"
        # Fragment: optional #anchor
        r"(#[A-Za-z0-9._~!$&'()*+,;=:@%/\-]+)?$"
    )

    Action: StringType = pa.Field(
        nullable=False,
        isin_spark={"allowed_values": ["upsert", "delete"]},
    )
    realm: StringType = pa.Field(
        nullable=False,
        isin_spark={"allowed_values": ["Next", "fatface"]},
    )
    territory: StringType = pa.Field(
        nullable=False,
        isin_spark={"allowed_values": ["GB"]},
    )

    url: StringType = pa.Field(
        nullable=False,
        str_matches_spark={"pattern": _URL_PATH_PATTERN},
        unique_spark={"check": True},
    )

    masIdSlotsAndCMSContent: StringType = pa.Field(
        nullable=False,
        str_matches_spark={"pattern": _MASID_CMS_CONTENT_PATTERN},
    )

    class Config:
        """GlobalSolutionOutputModel config"""

        name = "global_solution_output"
        strict = True  # Enforce exact columns
        coerce = True  # Attempt type coercion
