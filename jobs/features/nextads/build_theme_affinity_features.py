"""Build Theme Affinity feature-store tables."""

from _registry_job import metadata_only_main


if __name__ == "__main__":
    metadata_only_main("build_theme_affinity_features")

