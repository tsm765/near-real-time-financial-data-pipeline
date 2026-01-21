# scripts\run_prices_producer.ps1

# 1) Set project root explicitly
$projectRoot = "C:\Users\Talha\Projects\finance-kafka-project"
Set-Location $projectRoot

# 2) Load env file explicitly (for Synapse / tickers etc.)
$envFile = Join-Path $projectRoot "configs\.env"
Get-Content $envFile | ForEach-Object {
    if ($_ -match "^(.*?)=(.*)$") {
        [System.Environment]::SetEnvironmentVariable($matches[1], $matches[2])
    }
}

# 3) Hard-code Docker path (NO reliance on PATH)
$docker = "C:\Program Files\Docker\Docker\resources\bin\docker.exe"

# 4) Debug trace so we know Scheduler even called this script
$traceFile = Join-Path $projectRoot "logs\prices_scheduler_trace.log"
"[{0}] Task started (prices-producer)" -f (Get-Date -Format o) |
    Out-File -FilePath $traceFile -Append -Encoding utf8

# 5) Normal log for this run
$timestamp = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
$logFile = Join-Path $projectRoot ("logs\prices-producer_" + $timestamp + ".log")

"[$(Get-Date -Format o)] Starting prices-producer" |
    Out-File -FilePath $logFile -Encoding utf8

# 6) Run docker compose using the full docker.exe path
& docker compose run --rm prices-producer >> $logFile 2>&1



"[$(Get-Date -Format o)] Finished prices-producer (exit=$LASTEXITCODE)" |
    Out-File -FilePath $logFile -Append -Encoding utf8

exit $LASTEXITCODE
