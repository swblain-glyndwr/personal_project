create table marketingdata_prod.{schema}.{domain}_nextads_customer_cells_transient_latest (
    AccountNumber string not null,
    Cell string not null,
    CellValue string not null,
    rundate date not null,
  constraint pk_{domain}_nextads_customer_cells_transient_latest primary key (
    AccountNumber,
    Cell
    )
)
partitioned by (Cell)