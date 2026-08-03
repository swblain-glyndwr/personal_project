create table {catalog}.{schema}.{client}_nextads_candidate_foundation_sources (
    CandidateFoundationSnapshotID string not null,
    CandidateFoundationAttemptID string not null,
    RunDate date not null,
    SourceName string not null,
    SourceRole string not null,
    SourceTable string not null,
    DeltaVersion bigint not null,
    SchemaVersion string not null,
    SchemaChecksum string not null,
    IsRequired boolean not null,
    CapturedAt timestamp not null,
  constraint pk_{client}_nextads_candidate_foundation_sources primary key (
    CandidateFoundationAttemptID,
    SourceName
    )
)
partitioned by (RunDate)
