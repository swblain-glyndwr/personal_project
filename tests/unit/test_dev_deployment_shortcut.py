from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_dev_deployment_shortcut_posts_json_after_confirmation():
    script = (
        PROJECT_ROOT / "devops/scripts/queue-dev-deployment.ps1"
    ).read_text()
    rest_call = script.rsplit('-Command "az"', maxsplit=1)[1]

    assert '"--headers"' in rest_call
    assert '"Content-Type=application/json"' in rest_call
    assert rest_call.index('"--headers"') < rest_call.index('"--body"')
    assert script.index("$PSCmdlet.ShouldProcess") < script.rindex(
        '-Command "az"'
    )
