CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{client}_nextads_advert_advert_association (
    ViewUniqueAdID STRING NOT NULL,
    ViewCMSPageID STRING NOT NULL, 
    AtbUniqueAdID STRING NOT NULL,
    AtbCMSPageID STRING NOT NULL, 
    number_views_atbs double, 
    number_views double,
    number_atbs double,
    support_views double,
    support_atbs double,
    support_views_atbs double,
    cosine_similarity double,
    lift double,
    lift_adjusted double,
    lift_adjusted_ranking int,
    intersection_count int,
    overlap_proportion double NOT NULL,
    HomePage boolean not null, 
    OrderComplete boolean not null,
    ProductListingPage boolean not null,
    ShoppingBag boolean not null, 
    rundate date not null,
    constraint pk_{client}_nextads_advert_advert_association primary key (
        viewuniqueadid,
        atbuniqueadid,
        rundate)
)
USING DELTA
PARTITIONED BY (viewuniqueadid, rundate)
TBLPROPERTIES('delta.enableChangeDataFeed'= 'true' )
;

