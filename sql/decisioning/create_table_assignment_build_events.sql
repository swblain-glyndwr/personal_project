create table {catalog}.{schema}.{client}_nextads_assignment_build_events (
    BuildRunID string not null,
    Route string not null,
    Scope string not null,
    Status string not null,
    RowCount bigint not null,
    BuildDate date not null,
    TaskRunID bigint not null,
    ExecutionCount int not null,
    CompletedAt timestamp not null,
  constraint chk_{client}_nextads_assignment_build_events_route
    check (Route in ('v1', 'v2')),
  constraint chk_{client}_nextads_assignment_build_events_status
    check (Status in ('READY', 'NO_ADS')),
  constraint chk_{client}_nextads_assignment_build_events_row_count
    check (
      (Status = 'READY' and RowCount > 0)
      or (Status = 'NO_ADS' and RowCount = 0)
    ),
  constraint chk_{client}_nextads_assignment_build_events_execution_count
    check (ExecutionCount >= 0),
  constraint chk_{client}_nextads_assignment_build_events_task_run_id
    check (TaskRunID > 0)
)
partitioned by (BuildDate)
tblproperties ('delta.appendOnly' = 'true')
