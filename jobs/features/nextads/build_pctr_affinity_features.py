"""Build account-advert and session-context feature-store tables."""

from _registry_job import metadata_only_main


if __name__ == "__main__":
    metadata_only_main("build_pctr_affinity_features")

