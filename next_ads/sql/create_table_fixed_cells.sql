create table {table} (
    AccountNumber string not null,
    FallowControl bool not null,
    AlgoDivision string not null,
    MacroLocation string not null,
    MacroLocationCell string not null,
    rundate date not null,
  constraint pk_{table_name} primary key (
    AccountNumber,
    FallowControl,
    AlgoDivision,
    MacroLocation,
    MacroLocationCell
    )
)
partitioned by (AlgoDivision)