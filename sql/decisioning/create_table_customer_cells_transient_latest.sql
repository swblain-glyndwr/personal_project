create table {catalog}.{schema}.{client}_nextads_customer_cells_transient_latest (
    AccountNumber string not null,
    Cell string not null,
    CellValue string not null,
    rundate date not null,
  constraint pk_{client}_nextads_customer_cells_transient_latest primary key (
    AccountNumber,
    Cell
    )
)
partitioned by (Cell)