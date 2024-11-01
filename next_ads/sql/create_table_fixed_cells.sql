create table marketingdata_prod.ds_sandbox.next_uk_nextads_fixed_cells (
    AccountNumber string not null,
    FallowControl boolean not null,
    AlgoDivision string not null,
    MacroLocation string not null,
    MacroLocationCell string not null,
    rundate date not null,
  constraint pk_next_uk_nextads_fixed_cells primary key (
    AccountNumber,
    FallowControl,
    AlgoDivision,
    MacroLocation,
    MacroLocationCell
    )
)
partitioned by (AlgoDivision)