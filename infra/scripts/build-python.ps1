$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Root = (Resolve-Path (Join-Path $ScriptDir "../..")).Path
$OutDir = Join-Path $Root "apps/desktop/resources/backend"
$BackendExe = Join-Path $OutDir "server.exe"
$StartupTimeoutMs = 60000

Write-Host "[build-python] Upgrading pip..."
python -m pip install --upgrade pip
if ($LASTEXITCODE -ne 0) {
  throw "pip upgrade failed with exit code $LASTEXITCODE"
}

Write-Host "[build-python] Installing backend dependencies + pyinstaller..."
pip install pyinstaller -r (Join-Path $Root "backend/requirements.txt")
if ($LASTEXITCODE -ne 0) {
  throw "dependency installation failed with exit code $LASTEXITCODE"
}

New-Item -ItemType Directory -Path $OutDir -Force | Out-Null

Write-Host "[build-python] Building standalone backend executable..."
pyinstaller (Join-Path $Root "backend/app.py") `
  --onefile `
  --name server `
  --distpath $OutDir `
  --workpath "$env:RUNNER_TEMP/pyinstaller-build" `
  --specpath "$env:RUNNER_TEMP/pyinstaller-spec"
if ($LASTEXITCODE -ne 0) {
  throw "pyinstaller build failed with exit code $LASTEXITCODE"
}

if (-not (Test-Path $BackendExe)) {
  throw "Missing backend executable: $BackendExe"
}

$healthPort = Get-Random -Minimum 20000 -Maximum 50000
$backendProc = $null

try {
  Write-Host "[build-python] Running backend smoke test on port $healthPort..."
  $backendProc = Start-Process -FilePath $BackendExe -ArgumentList @("--port", "$healthPort") -PassThru -WindowStyle Hidden

  $healthy = $false
  for ($i = 0; $i -lt ($StartupTimeoutMs / 250); $i++) {
    Start-Sleep -Milliseconds 250
    try {
      $response = Invoke-WebRequest -Uri "http://127.0.0.1:$healthPort/api/health" -UseBasicParsing -TimeoutSec 2
      if ($response.StatusCode -eq 200) {
        $healthy = $true
        break
      }
    } catch {
      if ($backendProc.HasExited) {
        throw "Backend executable exited during smoke test with code $($backendProc.ExitCode)"
      }
    }
  }

  if (-not $healthy) {
    throw "Backend smoke test failed: /api/health did not become ready within $StartupTimeoutMs ms"
  }
} finally {
  if ($backendProc -and -not $backendProc.HasExited) {
    Stop-Process -Id $backendProc.Id -Force
  }
}

Write-Host "[build-python] Done. Binary at $BackendExe"
