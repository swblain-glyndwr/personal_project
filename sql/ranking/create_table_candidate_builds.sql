create table {catalog}.{schema}.{client}_nextads_candidate_builds (
    CandidateBuildID string not null,
    CandidateBuildAttemptID string not null,
    RunDate date not null,
    Route string not null,
    OutputGrain string not null,
    PortfolioID string not null,
    PortfolioAttemptID string not null,
    CandidateFoundationSnapshotID string not null,
    ControlTable string not null,
    ControlDeltaVersion bigint not null,
    CandidateContractVersion string not null,
    CandidatePolicyVersion string not null,
    CandidatePolicyChecksum string not null,
    ProviderBindingsJSON string not null,
    Status string not null,
    EntryCount int not null,
    OutputBindingsJSON string not null,
    GitCommit string not null,
    RuntimeMs bigint not null,
    TaskRunID bigint not null,
    ExecutionCount int not null,
    CompletedAt timestamp not null,
  constraint pk_{client}_nextads_candidate_builds primary key (
    CandidateBuildAttemptID
    )
)
partitioned by (RunDate)
