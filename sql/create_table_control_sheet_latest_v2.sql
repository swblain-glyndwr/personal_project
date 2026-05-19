create table {catalog}.{schema}.{client}_nextads_control_sheet_latest_v2 (
  UniqueAdID STRING NOT NULL,
  UniqueAdIDPremium STRING,
  CMSPageID STRING,
  PotNumber STRING NOT NULL,
  CampaignNumber STRING,
  Title STRING,
  AlgoDivision STRING,
  TradeDivision STRING,
  Brand STRING,
  MASIDToken STRING,
  Location STRING NOT NULL,
  Segment STRING,
  AdDriver STRING,
  TemplateName STRING,
  StartDate DATE,
  EndDate DATE,
  AudienceOnly INT,
  URL STRING,
  Items STRING,
  Tags STRING,
  Themes STRING,
  AdVariant STRING,
  rundate DATE NOT NULL,
  constraint pk_{client}_nextads_control_sheet_latest_v2 primary key (
    UniqueAdID,
    Location)
)
partitioned by (Location)