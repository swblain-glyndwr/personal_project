create table {catalog}.{schema}.{client}_nextads_scoring_foundation_run_contexts (
    ContextSlot string not null,
    OrchestrationRunID bigint not null,
    FoundationID string not null,
    FoundationVersion string not null,
    ScoringFoundationBuildID string not null,
    ScoringFoundationBuildAttemptID string not null,
    InputSnapshotID string not null,
    InputSnapshotAttemptID string not null,
    RunDate date not null,
    BindingsJSON string not null,
    Capability string not null,
    ContractVersion string not null,
    InvocationChecksum string not null,
    Status string not null,
    ExpiresAt timestamp not null,
    TaskRunID bigint not null,
    ExecutionCount int not null,
    ActivatedAt timestamp not null,
  constraint pk_{client}_nextads_scoring_foundation_run_contexts primary key (
    ContextSlot
    )
)
