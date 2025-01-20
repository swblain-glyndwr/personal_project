create table marketingdata_prod.{schema}.{domain}_nextads_customer_cells_fixed_history (
    AccountNumber string not null,
    FallowControl string not null,
    HomePageTest1 string not null,
    ShoppingBagTest1 string not null,
    OrderCompleteTest1 string not null,
    LandingPageTest1 string not null,
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
    RunDateFrom date not null,
    RunDateTo date not null,
    ControlCycle int not null,
  constraint pk_{domain}_nextads_customer_cells_fixed_history primary key (
    AccountNumber,
    ControlCycle
    )
)
partitioned by (ControlCycle)