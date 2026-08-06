create table {catalog}.{schema}.{client}_nextads_score_provider_signals (
    ProviderBuildID string not null,
    AccountNumber string not null,
    EntityType string not null,
    EntityID string not null,
    ProviderID string not null,
    RunDate date not null,
    RawScore double not null,
    Score double not null,
    ProviderRank int not null,
  constraint pk_{client}_nextads_score_provider_signals primary key (
    ProviderBuildID,
    AccountNumber,
    EntityType,
    EntityID
    ),
  constraint nextads_provider_raw_score_finite check (
    RawScore between -1.7976931348623157E308 and 1.7976931348623157E308
    ),
  constraint nextads_provider_score_finite check (
    Score between -1.7976931348623157E308 and 1.7976931348623157E308
    ),
  constraint nextads_provider_rank_valid check (ProviderRank >= 1)
)
partitioned by (RunDate, ProviderID)
