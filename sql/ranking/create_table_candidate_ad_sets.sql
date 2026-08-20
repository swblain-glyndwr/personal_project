create table {catalog}.{schema}.{client}_nextads_candidate_ad_sets (
    CandidateBuildID string not null,
    CandidateBuildAttemptID string not null,
    RunDate date not null,
    Route string not null,
    AdSetID string not null,
    ScopeType string not null,
    ScopeValue string not null,
    UniqueAdID string not null,
  constraint pk_{client}_nextads_candidate_ad_sets primary key (
    CandidateBuildAttemptID,
    AdSetID,
    ScopeType,
    ScopeValue,
    UniqueAdID
    )
)
partitioned by (RunDate, Route)
