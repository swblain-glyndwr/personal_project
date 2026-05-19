CREATE TABLE {catalog}.{schema}.{client}_nextads_realtime_results (
  SessionDate DATE NOT NULL,
  TreatmentGroup BOOLEAN NOT NULL,
  total_sessions BIGINT,
  total_orders BIGINT,
  total_revenue DOUBLE,
  RPV DOUBLE,
  CVR DOUBLE,
  AOV DOUBLE,
  rundate STRING,
  CONSTRAINT `pk_{client}_nextads_realtime_results` PRIMARY KEY (SessionDate, TreatmentGroup, rundate))
PARTITIONED BY (rundate)
