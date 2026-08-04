CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{client}_nextads_advert_items_catid (
    UniqueAdID STRING NOT NULL,
    CMSPageID STRING, 
    itemno STRING NOT NULL ,
    catid STRING,
    rundate date not null,
    constraint pk_{client}_nextads_advert_items_catid primary key (
        UniqueAdID,
        itemno,
        rundate)
)
