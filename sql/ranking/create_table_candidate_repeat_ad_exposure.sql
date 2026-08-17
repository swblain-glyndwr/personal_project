create table {catalog}.{schema}.{client}_nextads_candidate_repeat_ad_exposure (
    CandidateFoundationSnapshotID string not null,
    RunDate date not null,
    AccountNumber string not null,
    AdSeen string not null,
    sessions_seen_ad_in_last_7_days bigint not null,
    MultiSessionDownweightScore double not null,
  constraint pk_{client}_nextads_candidate_repeat_ad_exposure primary key (
    CandidateFoundationSnapshotID,
    AccountNumber,
    AdSeen
    )
)
partitioned by (RunDate)
