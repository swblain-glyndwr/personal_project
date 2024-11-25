create table marketingdata_prod.{schema}.next_uk_nextads_transient_cells (
    AccountNumber string not null,
    Cell string not null,
    CellValue string not null,
    rundate date not null,
  constraint pk_next_uk_nextads_transient_cells primary key (
    AccountNumber
    )
)
partitioned by (Cell)