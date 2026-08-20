create table {catalog}.{schema}.{client}_nextads_candidate_ad_feedback (
    CandidateFoundationSnapshotID string not null,
    RunDate date not null,
    UniqueAdID string not null,
    IncARPSAdjPct double not null,
  constraint pk_{client}_nextads_candidate_ad_feedback primary key (
    CandidateFoundationSnapshotID,
    UniqueAdID
    )
)
partitioned by (RunDate)
