CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{client}_nextads_items_catid (
    itemno STRING NOT NULL,
    catid STRING,
    rundate date not null,
    constraint pk_{client}_nextads_items_catid primary key (
        itemno,
        catid,
        rundate)
)