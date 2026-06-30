CREATE TABLE {catalog}.{schema}.{client}_nextads_control_sheet_plp_raw_latest (
  Location STRING NOT NULL,
  Page STRING,
  Screen STRING,
  PageGroup STRING,
  rundate DATE,
  CONSTRAINT `pk_{client}_nextads_control_sheet_plp_raw_latest` PRIMARY KEY (`Location`))
USING delta
PARTITIONED BY (Location)
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')

