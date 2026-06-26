CREATE TABLE {catalog}.{schema}.{client}_nextads_plp_gs_latest (
  Action STRING,
  realm STRING,
  territory STRING,
  url STRING,
  masIdSlotsAndCMSContent STRING,
  rundate DATE)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')