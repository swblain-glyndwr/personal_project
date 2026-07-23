CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{client}_nextads_advert_items_profile_similarity (
    UniqueAdID STRING NOT NULL,
    TargetUniqueAdID STRING NOT NULL,
    itemcount int,
    target_itemcount int,
    intersection_count int,
    overlap_proportion double NOT NULL,
    rundate date not null,
    constraint pk_{client}_nextads_advert_items_profile_similarity primary key (
        UniqueAdID,
        TargetUniqueAdID,
        rundate)
)

PARTITIONED BY (UniqueAdID)
