create table marketingdata_prod.{schema}.next_uk_nextads_test_cells (
    AccountNumber string not null,
    CellKey string not null,
    CellValue string not null,
    rundate date not null,
  constraint pk_next_uk_nextads_fixed_cells primary key (
    AccountNumber, CellKey
    )
)
partitioned by (CellKey)