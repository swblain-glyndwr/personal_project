create table {catalog}.{schema}.{client}_nextads_candidate_scores (
    CandidateBuildID string not null,
    CandidateBuildAttemptID string not null,
    RunDate date not null,
    Route string not null,
    PortfolioEntryID string not null,
    ServingSlot string not null,
    ExperimentID string not null,
    VariantID string not null,
    ProviderBuildID string not null,
    ProviderBuildAttemptID string not null,
    AccountNumber string not null,
    AdSetID string not null,
    UniqueAdID string not null,
    Score double not null,
    TriggerScore double,
    Rank int not null,
    CandidateID string not null,
  constraint pk_{client}_nextads_candidate_scores primary key (
    CandidateBuildAttemptID,
    CandidateID
    )
)
partitioned by (RunDate, Route)
