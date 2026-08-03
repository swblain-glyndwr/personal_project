create table {catalog}.{schema}.{client}_nextads_score_provider_run_contexts (
    ContextSlot string not null,
    OrchestrationRunID bigint not null,
    ProviderID string not null,
    ProviderBuildID string not null,
    ProviderBuildAttemptID string not null,
    InputSnapshotID string not null,
    RunDate date not null,
    ModelURI string not null,
    BindingsJSON string not null,
    Capability string not null,
    UseCase string not null,
    InvocationChecksum string not null,
    Status string not null,
    ExpiresAt timestamp not null,
    TaskRunID bigint not null,
    ExecutionCount int not null,
    ActivatedAt timestamp not null,
    ScoringFoundationBuildID string,
    ScoringFoundationBuildAttemptID string,
  constraint pk_{client}_nextads_score_provider_run_contexts primary key (
    ContextSlot
    )
)
