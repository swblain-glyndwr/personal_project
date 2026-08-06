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
    ),
  constraint nextads_candidate_score_finite check (
    Score between -1.7976931348623157E308 and 1.7976931348623157E308
    ),
  constraint nextads_candidate_trigger_finite check (
    TriggerScore is null or TriggerScore between
    -1.7976931348623157E308 and 1.7976931348623157E308
    ),
  constraint nextads_candidate_rank_valid check (Rank between 1 and 20)
)
partitioned by (RunDate, Route)
