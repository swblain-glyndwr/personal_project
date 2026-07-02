create table {catalog}.{schema}.{client}_nextads_customer_cells_latest (
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
    PageTypeIsolation string not null,
    specialaccountindicator string,
    AlgoDivision string not null,
    Audience string not null,
    IsPremium int not null,
    rundate date not null,
  constraint pk_{client}_nextads_customer_cells_latest primary key (
    AccountNumber
    )
)
partitioned by (FallowControl)