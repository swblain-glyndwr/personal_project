create table {catalog}.{schema}.{client}_nextads_scoring_input_snapshots (
    InputSnapshotID string not null,
    InputSnapshotAttemptID string not null,
    RunDate date not null,
    InputSchemaVersion string not null,
    GitCommit string not null,
    Status string not null,
    SourceCount int not null,
    WarningCount bigint not null,
    TaskRunID bigint not null,
    ExecutionCount int not null,
    CompletedAt timestamp not null,
  constraint pk_{client}_nextads_scoring_input_snapshots primary key (
    InputSnapshotAttemptID
    )
)
partitioned by (RunDate)
