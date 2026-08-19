[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = "Medium")]
param(
    [string]$Branch,
    [switch]$NoOpen
)

$ErrorActionPreference = "Stop"

$organization = "https://dev.azure.com/Next-Technology"
$project = "DirectoryMarketing.Personalisation"
$pipelineName = "mktg-next-ads-ci-cd"
$azureDevOpsResource = "499b84ac-1321-427f-aa17-267ca6975798"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$bodyFile = $null

function Invoke-CheckedCommand {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Command,
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments
    )

    $output = & $Command @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Command failed with exit code $LASTEXITCODE"
    }
    return $output
}

Push-Location $repoRoot
try {
    if (-not $Branch) {
        $Branch = (
            Invoke-CheckedCommand `
                -Command "git" `
                -Arguments @("branch", "--show-current")
        ).Trim()
    }
    if (-not $Branch) {
        throw "No branch was supplied and the repository is in detached HEAD state."
    }

    Invoke-CheckedCommand `
        -Command "git" `
        -Arguments @("fetch", "origin", $Branch) | Out-Null

    $remoteCommit = (
        Invoke-CheckedCommand `
            -Command "git" `
            -Arguments @(
                "rev-parse",
                "--verify",
                "origin/$Branch`^{commit}"
            )
    ).Trim()

    $currentBranch = (
        Invoke-CheckedCommand `
            -Command "git" `
            -Arguments @("branch", "--show-current")
    ).Trim()
    if ($currentBranch -eq $Branch) {
        $localCommit = (
            Invoke-CheckedCommand `
                -Command "git" `
                -Arguments @("rev-parse", "HEAD")
        ).Trim()
        if ($localCommit -ne $remoteCommit) {
            throw (
                "Local HEAD is not the latest pushed commit on origin/$Branch. " +
                "Push the branch before deploying."
            )
        }

        $workingChanges = @(
            Invoke-CheckedCommand `
                -Command "git" `
                -Arguments @("status", "--porcelain")
        )
        if ($workingChanges.Count -gt 0) {
            Write-Warning (
                "Uncommitted changes are not included. The pipeline will use " +
                "$remoteCommit from origin/$Branch."
            )
        }
    }

    $pipelineId = (
        Invoke-CheckedCommand `
            -Command "az" `
            -Arguments @(
                "pipelines",
                "show",
                "--name",
                $pipelineName,
                "--organization",
                $organization,
                "--project",
                $project,
                "--query",
                "id",
                "--output",
                "tsv",
                "--only-show-errors"
            )
    ).Trim()
    if (-not $pipelineId) {
        throw "Could not resolve Azure DevOps pipeline $pipelineName."
    }

    $body = @{
        resources = @{
            repositories = @{
                self = @{
                    refName = "refs/heads/$Branch"
                    version = $remoteCommit
                }
            }
        }
        templateParameters = @{
            destroyBundle = $false
            recreateDevIntegrationTables = $false
        }
        stagesToSkip = @(
            "DestroyDEV"
            "DestroyDEVIntegration"
            "DeployDEVIntegration"
            "InitializeDEVIntegrationTables"
            "DeployDEVFeatureStore"
            "DestroyPREPROD"
            "DeployPREPROD"
            "InitializePREPRODTables"
            "SmokePREPRODDependencies"
            "DestroyPROD"
            "DeployPROD"
        )
    } | ConvertTo-Json -Depth 10 -Compress

    Write-Host "Queueing CI and DEV deployment"
    Write-Host "Branch: $Branch"
    Write-Host "Commit: $remoteCommit"

    if (-not $PSCmdlet.ShouldProcess(
        "$Branch at $remoteCommit",
        "Queue CI and DEV deployment"
    )) {
        return
    }

    $bodyFile = Join-Path (
        [System.IO.Path]::GetTempPath()
    ) "nextads-dev-deployment-$([System.Guid]::NewGuid().ToString('N')).json"
    [System.IO.File]::WriteAllText(
        $bodyFile,
        $body,
        [System.Text.UTF8Encoding]::new($false)
    )

    $uri = (
        "$organization/$project/_apis/pipelines/$pipelineId/runs" +
        "?api-version=7.1"
    )
    $responseJson = (
        Invoke-CheckedCommand `
            -Command "az" `
            -Arguments @(
                "rest",
                "--method",
                "post",
                "--uri",
                $uri,
                "--resource",
                $azureDevOpsResource,
                "--headers",
                "Content-Type=application/json",
                "--body",
                "@$bodyFile",
                "--output",
                "json",
                "--only-show-errors"
            )
    ) -join [Environment]::NewLine
    $response = $responseJson | ConvertFrom-Json
    $runUrl = $response._links.web.href

    Write-Host "Queued pipeline run $($response.id)"
    Write-Host $runUrl

    if (-not $NoOpen -and $runUrl) {
        Start-Process $runUrl
    }
} finally {
    if ($bodyFile -and (Test-Path -LiteralPath $bodyFile)) {
        Remove-Item -LiteralPath $bodyFile -Force
    }
    Pop-Location
}
