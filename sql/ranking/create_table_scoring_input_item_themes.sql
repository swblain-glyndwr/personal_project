create table {catalog}.{schema}.{client}_nextads_scoring_input_item_themes (
    InputSnapshotID string not null,
    RunDate date not null,
    pid string not null,
    theme string not null,
    theme_rank int not null,
  constraint pk_{client}_nextads_scoring_input_item_themes primary key (
    InputSnapshotID,
    pid,
    theme
    )
)
partitioned by (RunDate)
