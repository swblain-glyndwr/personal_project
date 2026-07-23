create table {catalog}.{schema}.{client}_nextads_theme_scoring_events_latest_v2 (
    AccountNumber string not null,
    EventDate date not null,
    EventType string not null,
    EventWeight float,
    PID string not null,
    ItemTitle string,
    Theme string not null,
    rundate date not null,
  constraint pk_{client}_nextads_theme_scoring_events_latest_v2 primary key (
    AccountNumber,
    EventDate,
    EventType,
    PID,
    Theme
    )
)
partitioned by (EventDate)
