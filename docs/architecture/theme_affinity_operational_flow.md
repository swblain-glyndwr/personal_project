# Theme Affinity Operational Flow

Theme Affinity is a production model route. It is scheduled separately from the
main candidate-build route, and its output tables can feed model-building work
such as Feature Store.

```mermaid
flowchart TD
  subgraph route["mktg_next_uk_nextads_theme_affinity"]
    prep["predict_data_prep<br/>DLT / Lakeflow pipeline"]
    publish["publish_dlt_outputs"]
    dlt_check["sense_check_dlt_data"]
    predict["model_predict<br/>loads configured model URI"]
    clean["clean_output"]
    output_check["sense_check_model_outputs"]
  end

  subgraph outputs["Theme Affinity output tables"]
    ranked["ranked"]
    advanced["advanced_features"]
    customer_features["customer_features"]
    customer_segments["customer_segments"]
    popularity["popularity_metrics"]
    half["half / prediction output"]
  end

  subgraph consumers["Consumers"]
    current_ops["Current operational NextAds route"]
    fs["DEV_FEATURE_STORE<br/>model-building feature refresh"]
    monitoring["Model and data quality monitoring"]
  end

  prep --> publish
  prep --> dlt_check
  publish --> ranked
  publish --> advanced
  publish --> customer_features
  publish --> customer_segments
  publish --> popularity
  publish --> predict
  predict --> half
  predict --> clean --> output_check

  ranked --> fs
  advanced --> fs
  customer_features --> fs
  customer_segments --> fs
  popularity --> fs
  half --> fs

  ranked --> current_ops
  half --> current_ops
  ranked --> monitoring
  half --> monitoring
```

The Feature Store route reads Theme Affinity outputs as stable sources. It does
not replace this operational route or change the production model URI.
