create table {catalog}.{schema}.{client}_nextads_candidate_foundation_builds (
    CandidateFoundationSnapshotID string not null,
    CandidateFoundationAttemptID string not null,
    RunDate date not null,
    ContractVersion string not null,
    SourceBindingsJSON string not null,
    OutputBindingsJSON string not null,
    WarningCount bigint not null,
    Status string not null,
    FallbackSourceSnapshotID string,
    FallbackSourceRunDate date,
    TaskRunID bigint not null,
    ExecutionCount int not null,
    GitCommit string not null,
    CompletedAt timestamp not null,
  constraint pk_{client}_nextads_candidate_foundation_builds primary key (
    CandidateFoundationAttemptID
    )
)
partitioned by (RunDate)
