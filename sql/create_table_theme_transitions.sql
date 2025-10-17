create table marketingdata_prod.{schema}.{client}_nextads_theme_transitions (
    theme string not null,
    next_theme string not null,
    transition_freq decimal(12,2) not null,
    theme_total int not null,
    probability decimal(10,9) not null,
    base_probability decimal(10,9) not null,
    probability_rebased decimal(10,9) not null,
    rundate date not null,
  constraint pk_{client}_nextads_theme_transitions primary key (
    theme,
    next_theme,
    rundate
    )
)
partitioned by (theme)