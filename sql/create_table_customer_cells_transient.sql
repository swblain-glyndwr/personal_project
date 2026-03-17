create table {catalog}.{schema}.{client}_nextads_customer_cells_transient (
    AccountNumber string not null,
    Cell string not null,
    CellValue string not null,
    rundate date not null,
  constraint pk_{client}_nextads_customer_cells_transient primary key (
    AccountNumber,
    Cell,
    rundate
    )
)
partitioned by (rundate)