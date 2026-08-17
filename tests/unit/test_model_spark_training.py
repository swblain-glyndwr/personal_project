import hashlib

import pytest

from next_ads.model_development.spark_training import (
    artifact_directory_digest,
)


def test_artifact_digest_includes_relative_paths_and_file_bytes(tmp_path):
    model = tmp_path / "model"
    data = model / "data"
    data.mkdir(parents=True)
    (model / "MLmodel").write_text("model metadata")
    (data / "part-00000").write_bytes(b"model bytes")

    first = artifact_directory_digest(model)
    second = artifact_directory_digest(model)

    assert first == second
    assert len(first) == hashlib.sha256().digest_size * 2
    (data / "part-00000").write_bytes(b"changed model bytes")
    assert artifact_directory_digest(model) != first


def test_artifact_digest_rejects_empty_or_missing_artifacts(tmp_path):
    empty = tmp_path / "empty"
    empty.mkdir()

    with pytest.raises(ValueError, match="empty"):
        artifact_directory_digest(empty)
    with pytest.raises(ValueError, match="does not exist"):
        artifact_directory_digest(tmp_path / "missing")
