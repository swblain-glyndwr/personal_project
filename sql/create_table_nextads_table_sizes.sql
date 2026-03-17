CREATE TABLE {catalog}.{schema}.nextads_table_sizes (
  table_key STRING NOT NULL,
  table_size_GB DOUBLE,
  table_path STRING,
  schema STRING NOT NULL,
  rundate DATE,
  CONSTRAINT `pk_nextads_table_sizes` PRIMARY KEY (`schema`, `table_key`))
USING delta
PARTITIONED BY (schema, table_key)
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
