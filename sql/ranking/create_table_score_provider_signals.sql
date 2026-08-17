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
    )
)
partitioned by (RunDate, ProviderID)
