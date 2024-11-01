create table {table} (
    AccountNumber string not null,
    Location string not null,
    MacroLocation string not null,
    MacroLocationCell string not null,
    ChampionChallenger string,
    RandomUniqueAdID string,
    RandomMASID string not null,
    BestUniqueAdID string,
    BestMASID string,
    BestUniqueAdIDChallenger string,
    BestMASIDChallenger string,
    UniqueAdID string,
    MASID string not null,
    rundate date not null,
  constraint pk_{table_name} primary key (
    AccountNumber,
    Location
    )
)
partitioned by (Location)