create table {catalog}.{schema}.{client}_nextads_scoring_input_snapshot_sources (
    InputSnapshotID string not null,
    InputSnapshotAttemptID string not null,
    RunDate date not null,
    SourceName string not null,
    SourceRole string not null,
    SourceTable string not null,
    DeltaVersion bigint not null,
    SchemaVersion string not null,
    SchemaChecksum string not null,
    IsRequired boolean not null,
    AcceptedTable string,
    AcceptedDeltaVersion bigint,
    AcceptedSchemaChecksum string,
    WriteReceiptID string,
    TaskRunID bigint not null,
    ExecutionCount int not null,
    CapturedAt timestamp not null,
  constraint pk_{client}_nextads_scoring_input_snapshot_sources primary key (
    InputSnapshotAttemptID,
    SourceName
    )
)
partitioned by (RunDate)
