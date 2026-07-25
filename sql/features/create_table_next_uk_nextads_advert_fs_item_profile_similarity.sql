CREATE TABLE {catalog}.{schema}.{client}_nextads_advert_fs_item_profile_similarity (
    reference_date DATE NOT NULL,
    SourceUniqueAdID string not null,
    TargetUniqueAdID string not null,
    source_item_count int,
    target_item_count int,
    intersection_count int,
    overlap_proportion double,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
  constraint pk_{client}_nextads_advert_fs_item_profile_similarity primary key (
    reference_date ,
    SourceUniqueAdID,
    TargetUniqueAdID
    )
)
USING DELTA
CLUSTER BY (reference_date ,
    SourceUniqueAdID,
    TargetUniqueAdID)
