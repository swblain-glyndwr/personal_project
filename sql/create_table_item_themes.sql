create table marketingdata_prod.{schema}.{client}_nextads_item_themes (
    pid string not null,
    theme string not null,
    theme_rank int not null,
    rundate date not null,
  constraint pk_{client}_nextads_item_themes primary key (
    pid,
    theme,
    rundate
    )
)
partitioned by (rundate)