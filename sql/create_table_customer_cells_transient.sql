create table marketingdata_prod.{schema}.{domain}_nextads_customer_cells_transient (
    AccountNumber string not null,
    Cell string not null,
    CellValue string not null,
    rundate date not null,
  constraint pk_{domain}_nextads_customer_cells_transient primary key (
    AccountNumber,
    Cell,
    rundate
    )
)
partitioned by (rundate)