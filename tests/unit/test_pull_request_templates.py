from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]

PULL_REQUEST_TEMPLATES = (
    ".azuredevops/pull_request_template.md",
    ".azuredevops/pull_request_template/branches/develop.md",
    ".azuredevops/pull_request_template/branches/release.md",
    ".azuredevops/pull_request_template/branches/main.md",
)


@pytest.mark.parametrize("template_path", PULL_REQUEST_TEMPLATES)
def test_pull_request_templates_do_not_repeat_target_branch(template_path):
    template = (PROJECT_ROOT / template_path).read_text()

    assert "### Target Branch" not in template
    assert "- Target branch:" not in template
    assert "Confirm this follows the route" not in template


@pytest.mark.parametrize("template_path", PULL_REQUEST_TEMPLATES)
def test_pull_request_templates_retain_review_context(template_path):
    template = (PROJECT_ROOT / template_path).read_text()

    assert "### Purpose" in template
    assert "### File Changes" in template
    assert "### Output and Deployment Impact" in template


def test_branch_templates_retain_route_specific_evidence_sections():
    develop = (
        PROJECT_ROOT / ".azuredevops/pull_request_template/branches/develop.md"
    ).read_text()
    release = (
        PROJECT_ROOT / ".azuredevops/pull_request_template/branches/release.md"
    ).read_text()
    main = (
        PROJECT_ROOT / ".azuredevops/pull_request_template/branches/main.md"
    ).read_text()

    assert "### Validation Evidence" in develop
    assert "### Release Candidate Scope" in release
    assert "### PREPROD Validation Plan" in release
    assert "### Release Evidence" in main
    assert "### Production Tag" in main
    assert "### PROD Deployment Plan" in main
