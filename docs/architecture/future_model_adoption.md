# Future Model Adoption

Future pCTR, LTR and direct-ad challenger work should reuse feature contracts
and the shared MLflow lifecycle without changing production decisioning by
default.

```mermaid
flowchart TD
  subgraph inputs["Reusable inputs"]
    fs_base["Feature Store base tables"]
    fs_model["Feature Store model-input tables"]
    labels["Label and result tables"]
    experiments["Experiment-specific analysis tables"]
  end

  subgraph model["Model-specific development"]
    pctr["pCTR model"]
    ltr["LTR / ranking model"]
    direct["Direct-ad challenger"]
    train["Train, evaluate and register through MLflow"]
  end

  subgraph evidence["Review evidence"]
    metrics["Metrics and artifacts"]
    model_version["Reviewed UC model version"]
    monitor["Monitoring and drift evidence"]
  end

  subgraph decisioning["Production decisioning boundary"]
    adapter["Champion/challenger or scoring adapter"]
    output["Model output or decisioning table"]
    delivery["Existing delivery route"]
  end

  fs_base --> pctr
  fs_model --> pctr
  fs_base --> ltr
  fs_model --> ltr
  fs_base --> direct
  labels --> train
  experiments --> train

  pctr --> train
  ltr --> train
  direct --> train
  train --> metrics
  train --> model_version
  model_version --> monitor

  model_version -. "release-controlled selection" .-> adapter
  adapter --> output --> delivery
```

Feature creation should stay in reusable feature contracts when the signal is
shared across models. Final scores, rankings, assignment choices and delivery
payloads should stay in model output, decisioning or delivery contracts.
