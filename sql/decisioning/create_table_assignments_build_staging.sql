create table {catalog}.{schema}.{client}_nextads_assignments_build_staging (
    BuildRunID string not null,
    TaskRunID bigint not null,
    ExecutionCount int not null,
    AccountNumber string not null,
    Location string not null,
    UniqueAdIDBasic string,
    UniqueAdIDBest string,
    UniqueAdIDBestChallenger string,
    UniqueAdIDNextGenAds string,
    Treatment string,
    UniqueAdIDMeasurement string,
    UniqueAdIDAssigned string not null,
    MASID string not null,
    rundate date not null,
  constraint chk_{client}_nextads_assignments_build_staging_task_run_id
    check (TaskRunID > 0),
  constraint chk_{client}_nextads_assignments_build_staging_execution_count
    check (ExecutionCount >= 0),
  constraint pk_{client}_nextads_assignments_build_staging primary key (
    BuildRunID,
    AccountNumber,
    Location)
)
partitioned by (BuildRunID, Location)
