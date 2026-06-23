create table {catalog}.{schema}.{client}_nextads_payload_latest(
  roamingprofileid BIGINT,
  next_ads STRUCT<
    AccountNumber: STRING NOT NULL,
    adFatigueImpressionThreshold: INT NOT NULL,
    experimentId: STRING NOT NULL,
    triggers: ARRAY<STRUCT<t: FLOAT, id: STRING>> NOT NULL,
    control: BOOLEAN NOT NULL,
    fragments: ARRAY<ARRAY<STRUCT<
      pageTypes: ARRAY<STRING>,
      enableAdFatigueRotation: BOOLEAN,
      fragmentIds: ARRAY<STRING>
    >>> NOT NULL,
    adsHash: STRING
  > NOT NULL,
  rundate date not null,
  constraint pk_{client}_nextads_payload_latest primary key (
    roamingprofileid,
    rundate
    )
)