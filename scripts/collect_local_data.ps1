param(
    [string]$DataRoot = "$PSScriptRoot\..\local_data",
    [string]$Start = "2016-10-24",
    [string]$End = "2026-10-23",
    [int]$BatchSize = 100,
    [int]$Batches = 1
)
$ErrorActionPreference = "Stop"
$Collector = Join-Path $PSScriptRoot "collect_local_data.py"
python $Collector --root $DataRoot init --start $Start --end $End
if ($LASTEXITCODE -ne 0) { throw "Could not initialize local collection." }
for ($i = 0; $i -lt $Batches; $i++) {
    python $Collector --root $DataRoot fetch --limit $BatchSize
    if ($LASTEXITCODE -notin @(0, 2)) { throw "Collection stopped unexpectedly." }
    # Exit 2 retains missing/failed jobs for review; it never labels a 404 a holiday.
}
python $Collector --root $DataRoot status
