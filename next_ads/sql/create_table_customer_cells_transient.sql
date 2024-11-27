create table marketingdata_prod.{schema}.next_uk_nextads_customer_cells_transient (
    AccountNumber string not null,
    Cell string not null,
    CellValue string not null,
    rundate date not null,
  constraint pk_next_uk_nextads_customer_cells_transient primary key (
    AccountNumber,
    rundate
    )
)
partitioned by (rundate)