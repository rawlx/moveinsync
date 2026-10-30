<#
Load test with gateway-failure injection against the Docker Compose stack.

  powershell -ExecutionPolicy Bypass -File deploy\loadtest\run-loadtest.ps1 [-Clients 10000] [-DurationSeconds 180]
      [-KillAfterSeconds 60] [-KillService gateway-2] [-SosPerSecond 2] [-NoKill] [-NoBuild]

Steps: (re)start the stack and wait for healthy replicas; snapshot docker stats; run the loadgen container (profile
"loadtest"); snapshot at its CONNECTED and STEADY phase markers; kill one gateway container KillAfterSeconds after
STEADY; snapshot during recovery; wait for the loadgen to finish; collect logs. The finally block always stops the
loadgen and restores the killed replica, even when the run fails or is interrupted.
Results: loadtest-results\<run id>\ (summary.txt, result.json from the loadgen; stats-*.txt, events.txt, logs).
#>
param(
    [int]$Clients = 10000,
    [int]$DurationSeconds = 180,
    [int]$KillAfterSeconds = 60,
    [string]$KillService = "gateway-2",
    [double]$SosPerSecond = 2,
    [int]$ConnectsPerSecond = 400,
    [switch]$NoKill,
    [switch]$NoBuild
)

$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root
$runId = "run-" + (Get-Date -Format "yyyyMMdd-HHmmss")
$results = Join-Path $root "loadtest-results\$runId"
New-Item -ItemType Directory -Force $results | Out-Null
$eventsFile = Join-Path $results "events.txt"

function Note([string]$text) {
    $line = "$((Get-Date).ToUniversalTime().ToString('o')) $text"
    Add-Content -Path $eventsFile -Value $line
    Write-Host $line
}

function Stats([string]$label) {
    $file = Join-Path $results "stats-$label.txt"
    docker stats --no-stream --format "{{.Name}}`t{{.CPUPerc}}`t{{.MemUsage}}`t{{.MemPerc}}`t{{.NetIO}}`t{{.PIDs}}" |
        Out-File -Encoding utf8 $file
    Note "docker stats snapshot '$label' -> $file"
}

function WaitForLog([string]$container, [string]$marker, [int]$timeoutSeconds) {
    $deadline = (Get-Date).AddSeconds($timeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $logs = docker logs $container 2>&1 | Out-String
        if ($logs -match [regex]::Escape($marker)) { return $true }
        $state = docker inspect -f "{{.State.Running}}" $container 2>$null
        if ($state -ne "true") { return $false }
        Start-Sleep -Seconds 2
    }
    return $false
}

$loadgen = "loadgen-$runId"
$killed = $null
try {
    Note "run $runId clients=$Clients duration=${DurationSeconds}s killAfter=${KillAfterSeconds}s kill=$KillService noKill=$NoKill"
    docker info --format "Docker {{.ServerVersion}}, VM CPUs {{.NCPU}}, VM memory {{.MemTotal}}" | Out-File -Encoding utf8 (Join-Path $results "docker-info.txt")
    if ($NoBuild) { docker compose up -d --wait } else { docker compose up -d --build --wait }
    if ($LASTEXITCODE -ne 0) { throw "compose stack did not become healthy" }
    Note "stack healthy"
    Stats "baseline"

    $env:LOADTEST_RUN_ID = $runId
    $env:LOADTEST_CLIENTS = "$Clients"
    $env:LOADTEST_DURATION_S = "$DurationSeconds"
    $env:LOADTEST_SOS_PER_SECOND = "$SosPerSecond"
    $env:LOADTEST_CONNECTS_PER_SECOND = "$ConnectsPerSecond"
    docker compose --profile loadtest run -d --name $loadgen loadgen | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "loadgen did not start" }
    Note "loadgen started ($loadgen)"

    if (-not (WaitForLog $loadgen "LOADTEST PHASE CONNECTED" 600)) { throw "loadgen never reached CONNECTED" }
    Note "loadgen CONNECTED"
    Stats "connected"
    if (-not (WaitForLog $loadgen "LOADTEST PHASE STEADY" 300)) { throw "loadgen never reached STEADY" }
    Note "loadgen STEADY"
    Stats "steady"

    if (-not $NoKill) {
        Start-Sleep -Seconds $KillAfterSeconds
        $target = (docker compose ps -q $KillService)
        Note "KILL $KillService ($target)"
        docker kill $target | Out-Null
        $killed = $KillService
        Note "killed $KillService"
        Start-Sleep -Seconds 5
        Stats "recovery-5s"
        Start-Sleep -Seconds 10
        Stats "recovery-15s"
    }

    $exit = docker wait $loadgen
    Note "loadgen exited with $exit"
    Stats "end"
}
finally {
    docker logs $loadgen *> (Join-Path $results "loadgen.log")
    docker compose logs --no-color gateway-1 gateway-2 gateway-3 *> (Join-Path $results "gateways.log")
    if ((docker inspect -f "{{.State.Running}}" $loadgen 2>$null) -eq "true") {
        Note "stopping loadgen (cleanup)"
        docker stop $loadgen | Out-Null
    }
    docker rm -f $loadgen *> $null
    if ($killed) {
        Note "restoring $killed"
        docker compose up -d --wait $killed | Out-Null
        Note "restored ${killed}: $(docker compose ps --format '{{.Service}} {{.Status}}' $killed)"
    }
    docker compose ps --format "{{.Service}}`t{{.Status}}" | Out-File -Encoding utf8 (Join-Path $results "compose-ps-final.txt")
    Note "results in $results"
}
