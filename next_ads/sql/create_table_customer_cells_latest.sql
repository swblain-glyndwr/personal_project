create table marketingdata_prod.{schema}.next_uk_nextads_customer_cells_fixed_latest (
    AccountNumber string not null,
    FallowControl string not null,
    HNTest1 string not null,
    SBTest1 string not null,
    OCTest1 string not null,
    LPTest1 string not null,
    AdHocABTest1 string not null,
    AdHocABTest2 string not null,
    AdHocABTest3 string not null,
    AdHocABTest4 string not null,
    AdHocABTest5 string not null,
    AdHocABTest6 string not null,
    AdHocABTest7 string not null,
    AdHocABTest8 string not null,
    AdHocABTest9 string not null,
    ChampionChallenger string not null,
    rundate date not null,
  constraint pk_next_uk_nextads_customer_cells_fixed_latest primary key (
    AccountNumber
    )
)
partitioned by (FallowControl)