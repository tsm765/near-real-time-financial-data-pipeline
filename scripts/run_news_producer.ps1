# scripts\run_news_producer.ps1

$projectRoot = "C:\Users\Talha\Projects\finance-kafka-project"
Set-Location $projectRoot

$envFile = Join-Path $projectRoot "configs\.env"
Get-Content $envFile | ForEach-Object {
    if ($_ -match "^(.*?)=(.*)$") {
        [System.Environment]::SetEnvironmentVariable($matches[1], $matches[2])
    }
}

$docker = "C:\Program Files\Docker\Docker\resources\bin\docker.exe"

$traceFile = Join-Path $projectRoot "logs\news_scheduler_trace.log"
"[{0}] Task started (news-producer)" -f (Get-Date -Format o) |
    Out-File -FilePath $traceFile -Append -Encoding utf8

$timestamp = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
$logFile = Join-Path $projectRoot ("logs\news-producer_" + $timestamp + ".log")

"[$(Get-Date -Format o)] Starting news-producer" |
    Out-File -FilePath $logFile -Encoding utf8

& docker compose run --rm news-producer >> $logFile 2>&1




"[$(Get-Date -Format o)] Finished news-producer (exit=$LASTEXITCODE)" |
    Out-File -FilePath $logFile -Append -Encoding utf8

exit $LASTEXITCODE
