create table marketingdata_prod.{schema}.next_uk_nextads_assignments_latest (
    AccountNumber string not null,
    Location string not null,
    MacroLocation string not null,
    MacroLocationCell string not null,
    AdHocAB1 string,
    AdHocAB2 string,
    AdHocAB3 string,
    AdHocAB4 string,
    ChampionChallenger string,
    AlgoDivision string,
    UniqueAdIDShown string not null,
    MASID string not null,
    rundate date not null,
  constraint pk_next_uk_nextads_assignments_latest primary key (
    AccountNumber,
    Location)
)
partitioned by (Location)