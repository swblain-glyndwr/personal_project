create table {catalog}.{schema}.{client}_nextads_item_themes_latest_v2 (
    pid string not null,
    theme string not null,
    theme_rank int not null,
    rundate date not null,
  constraint pk_{client}_nextads_item_themes_latest_v2 primary key (
    pid,
    theme
    )
)
partitioned by (theme)
