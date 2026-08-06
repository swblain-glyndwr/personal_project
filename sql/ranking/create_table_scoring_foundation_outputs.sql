create table {catalog}.{schema}.{client}_nextads_scoring_foundation_outputs (
    ScoringFoundationBuildID string not null,
    ScoringFoundationBuildAttemptID string not null,
    RunDate date not null,
    OutputName string not null,
    SourceTable string not null,
    SourceDeltaVersion bigint,
    SourceSchemaChecksum string not null,
    OutputTable string not null,
    OutputDeltaVersion bigint not null,
    OutputSchemaVersion string not null,
    OutputSchemaChecksum string not null,
    IsRequired boolean not null,
    RowCount bigint not null,
    WriteReceiptID string not null,
    GitCommit string not null,
    WriteDurationMs bigint not null,
    RetryCount int not null,
    PublishedAt timestamp not null,
  constraint pk_{client}_nextads_scoring_foundation_outputs primary key (
    ScoringFoundationBuildAttemptID,
    OutputName
    )
)
partitioned by (RunDate)
