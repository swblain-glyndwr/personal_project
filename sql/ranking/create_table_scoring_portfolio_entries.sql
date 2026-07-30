create table {catalog}.{schema}.{client}_nextads_scoring_portfolio_entries (
    PortfolioID string not null,
    PortfolioAttemptID string not null,
    PortfolioEntryID string not null,
    RunDate date not null,
    ProviderBuildID string not null,
    PolicyRole string not null,
    ExecutionMode string not null,
    ServingSlot string,
    Priority int not null,
    TaskRunID bigint not null,
    ExecutionCount int not null,
  constraint pk_{client}_nextads_scoring_portfolio_entries primary key (
    PortfolioAttemptID,
    PortfolioEntryID
    )
)
partitioned by (RunDate)
