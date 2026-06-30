create table {catalog}.{schema}.{client}_nextads_theme_affinity_inference_log (
    inference_date date not null,
    inference_timestamp timestamp not null,
    model_id string not null,
    account_number string not null,
    theme string not null,
    prediction double not null,
    rank int,
    label int,
    label_observed_until date,
    label_updated_timestamp timestamp,
  constraint pk_{client}_nextads_theme_affinity_inference_log primary key (
    inference_date,
    model_id,
    account_number,
    theme
    )
)
