CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{client}_nextads_advert_items_profile_similarity (
    UniqueAdID STRING NOT NULL,
    CMSPageID STRING, 
    TargetUniqueAdID STRING NOT NULL,
    TargetCMSPageID STRING,
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
