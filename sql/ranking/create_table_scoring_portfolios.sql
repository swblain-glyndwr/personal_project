create table {catalog}.{schema}.{client}_nextads_scoring_portfolios (
    PortfolioID string not null,
    PortfolioAttemptID string not null,
    RunDate date not null,
    Capability string not null,
    UseCase string not null,
    Route string not null,
    PolicyID string not null,
    PolicyPriority int not null,
    Location string not null,
    PageType string not null,
    Audience string not null,
    CustomerCell string not null,
    ContractVersion string not null,
    Status string not null,
    EntryCount int not null,
    WarningCount bigint not null,
    TaskRunID bigint not null,
    ExecutionCount int not null,
    CompletedAt timestamp not null,
    FallbackSourcePortfolioID string,
    FallbackSourceRunDate date,
    FallbackSourceCompletedAt timestamp,
  constraint pk_{client}_nextads_scoring_portfolios primary key (
    PortfolioAttemptID
    )
)
partitioned by (RunDate)
