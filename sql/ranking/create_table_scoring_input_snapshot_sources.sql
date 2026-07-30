create table {catalog}.{schema}.{client}_nextads_scoring_input_snapshot_sources (
    InputSnapshotID string not null,
    InputSnapshotAttemptID string not null,
    RunDate date not null,
    SourceName string not null,
    SourceRole string not null,
    SourceTable string not null,
    DeltaVersion bigint not null,
    SchemaVersion string not null,
    IsRequired boolean not null,
    RowCount bigint not null,
    DistinctKeyCount bigint not null,
    NullKeyCount bigint not null,
    DuplicateKeyCount bigint not null,
    ContentChecksum string not null,
    TaskRunID bigint not null,
    ExecutionCount int not null,
    CapturedAt timestamp not null,
  constraint pk_{client}_nextads_scoring_input_snapshot_sources primary key (
    InputSnapshotAttemptID,
    SourceName
    )
)
partitioned by (RunDate)
