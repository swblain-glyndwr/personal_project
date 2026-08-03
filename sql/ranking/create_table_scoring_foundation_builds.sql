create table {catalog}.{schema}.{client}_nextads_scoring_foundation_builds (
    ScoringFoundationBuildID string not null,
    ScoringFoundationBuildAttemptID string not null,
    InputSnapshotID string not null,
    InputSnapshotAttemptID string not null,
    RunDate date not null,
    FoundationID string not null,
    FoundationVersion string not null,
    Capability string not null,
    ContractVersion string not null,
    InvocationChecksum string not null,
    RequiredOutputsJSON string not null,
    InputBindingsJSON string not null,
    PipelineID string,
    PipelineUpdateID string,
    PipelineTaskRunID bigint,
    PipelineUpdateType string,
    WarningCount bigint not null,
    Status string not null,
    TaskRunID bigint not null,
    ExecutionCount int not null,
    CompletedAt timestamp not null,
  constraint pk_{client}_nextads_scoring_foundation_builds primary key (
    ScoringFoundationBuildAttemptID
    )
)
partitioned by (RunDate)
