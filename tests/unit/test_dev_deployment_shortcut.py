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
    assert '"@$bodyFile"' in rest_call
    confirmation_index = script.index("$PSCmdlet.ShouldProcess")
    body_file_index = script.index("[System.IO.File]::WriteAllText")
    request_index = script.rindex('-Command "az"')
    assert confirmation_index < body_file_index < request_index


def test_dev_deployment_shortcut_always_removes_json_body_file():
    script = (
        PROJECT_ROOT / "devops/scripts/queue-dev-deployment.ps1"
    ).read_text()
    finally_block = script.rsplit("} finally {", maxsplit=1)[1]

    assert "Test-Path -LiteralPath $bodyFile" in finally_block
    assert "Remove-Item -LiteralPath $bodyFile -Force" in finally_block
