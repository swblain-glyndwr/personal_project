create table marketingdata_prod.{schema}.next_uk_nextads_assignments (
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
  constraint pk_next_uk_nextads_assignments primary key (
    AccountNumber,
    Location,
    rundate)
)
partitioned by (rundate)