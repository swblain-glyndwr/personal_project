create table {catalog}.{schema}.{client}_nextads_assignment_build_events (
    BuildRunID string not null,
    Route string not null,
    Scope string not null,
    Status string not null,
    RowCount bigint not null,
    BuildDate date not null,
    TaskRunID bigint not null,
    ExecutionCount int not null,
    CompletedAt timestamp not null
)
partitioned by (BuildDate)
