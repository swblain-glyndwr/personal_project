CREATE TABLE marketingdata_prod.{schema}.{client}_nextads_control_sheet_plp_raw (
  Location STRING NOT NULL,
  Page STRING,
  Screen STRING,
  PageGroup STRING,
  rundate DATE,
  CONSTRAINT `pk_{client}_nextads_control_sheet_plp_raw` PRIMARY KEY (`Location`))
USING delta
PARTITIONED BY (rundate)
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')