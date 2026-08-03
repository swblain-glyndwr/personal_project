create table {catalog}.{schema}.{client}_nextads_scoring_input_theme_mapping_raw (
    LandingID string not null,
    RunDate date not null,
    SourceRole string not null,
    SourceRowKey string not null,
    Theme string,
    TargetingAttributes string,
    ThemeType string,
    ThemeTypeRank string,
    AdType string,
    AdTypeRank string,
  constraint pk_{client}_nextads_scoring_input_theme_mapping_raw primary key (
    LandingID,
    SourceRole,
    SourceRowKey
    )
)
partitioned by (RunDate)
