create table marketingdata_prod.{schema}.next_uk_nextads_fixed_cells (
    AccountNumber string not null,
    FallowControl boolean not null,
    HN string not null,
    SB string not null,
    OC string not null,
    LP string not null,
    AdHocAB1 string,
    AdHocAB2 string,
    AdHocAB3 string,
    AdHocAB4 string,
    ChampionChallenger string,
    AlgoDivision string,
    rundate date not null,
  constraint pk_next_uk_nextads_fixed_cells primary key (
    AccountNumber
    )
)
partitioned by (AlgoDivision)