CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{client}_nextads_advert_advert_association (
    ViewUniqueAdID STRING NOT NULL,
    AtbUniqueAdID STRING NOT NULL,
    number_views_atbs double,
    number_views double,
    number_atbs double,
    support_views double,
    support_atbs double,
    support_views_atbs double,
    cosine_similarity double,
    lift double,
    lift_adjusted double,
    lift_adjusted_ranking double,
    intersection_count int,
    overlap_proportion double NOT NULL,
    -- Add in page types here
    rundate date not null,
    constraint pk_{client}_nextads_advert_advert_association primary key (
        ViewUniqueAdID,
        AtbUniqueAdID,
        rundate)
)

PARTITIONED BY (ViewUniqueAdID, rundate)
