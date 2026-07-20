$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Root = (Resolve-Path (Join-Path $ScriptDir "../..")).Path
$OutDir = Join-Path $Root "apps/desktop/resources/backend"
$BackendExe = Join-Path $OutDir "server.exe"
$StartupTimeoutMs = 60000
$BuildTempRoot = if ([string]::IsNullOrWhiteSpace($env:RUNNER_TEMP)) {
  Join-Path $Root ".tmp/pyinstaller"
} else {
  Join-Path $env:RUNNER_TEMP "sprklogs-pyinstaller"
}
$BuildWorkDir = Join-Path $BuildTempRoot "build"
$BuildSpecDir = Join-Path $BuildTempRoot "spec"

function Stop-PackagedBackend {
  Get-Process -Name "server" -ErrorAction SilentlyContinue |
    Where-Object {
      try {
        $_.Path -eq $BackendExe
      } catch {
        $false
      }
    } |
    Stop-Process -Force -ErrorAction SilentlyContinue
}

$PythonExecutable = "python"
$PythonPrefix = @()
$currentPythonVersion = python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>$null
if ($LASTEXITCODE -ne 0 -or $currentPythonVersion -notin @("3.11", "3.12")) {
  $PythonExecutable = $null
  if (Get-Command py -ErrorAction SilentlyContinue) {
    foreach ($candidate in @("3.11", "3.12")) {
      $candidateAvailable = $false
      try {
        & py "-$candidate" --version *> $null
        $candidateAvailable = $LASTEXITCODE -eq 0
      } catch {
        $candidateAvailable = $false
      }
      if ($candidateAvailable) {
        $PythonExecutable = "py"
        $PythonPrefix = @("-$candidate")
        break
      }
    }
  }
}

if (-not $PythonExecutable) {
  throw "Python 3.11 or 3.12 is required to package the backend."
}

function Invoke-Python {
  param([string[]]$Arguments)

  $allArguments = @($PythonPrefix) + $Arguments
  & $PythonExecutable @allArguments
}

Write-Host "[build-python] Using $PythonExecutable $PythonPrefix"
New-Item -ItemType Directory -Path $BuildTempRoot -Force | Out-Null

$VirtualEnvDir = Join-Path $BuildTempRoot "venv"
$BuildPython = Join-Path $VirtualEnvDir "Scripts/python.exe"
if (-not (Test-Path -LiteralPath $BuildPython)) {
  Write-Host "[build-python] Creating isolated build environment..."
  Invoke-Python @("-m", "venv", $VirtualEnvDir)
  if ($LASTEXITCODE -ne 0) {
    throw "virtual environment creation failed with exit code $LASTEXITCODE"
  }
}

Write-Host "[build-python] Upgrading pip..."
& $BuildPython -m pip install --upgrade pip
if ($LASTEXITCODE -ne 0) {
  throw "pip upgrade failed with exit code $LASTEXITCODE"
}

Write-Host "[build-python] Installing backend dependencies + pyinstaller..."
$installArguments = @(
  "-m",
  "pip",
  "install",
  "pyinstaller",
  "-r",
  (Join-Path $Root "backend/requirements.txt")
)
& $BuildPython @installArguments
if ($LASTEXITCODE -ne 0) {
  throw "dependency installation failed with exit code $LASTEXITCODE"
}

New-Item -ItemType Directory -Path $OutDir -Force | Out-Null
New-Item -ItemType Directory -Path $BuildWorkDir -Force | Out-Null
New-Item -ItemType Directory -Path $BuildSpecDir -Force | Out-Null

Stop-PackagedBackend
Write-Host "[build-python] Building standalone backend executable..."
$pyInstallerArguments = @(
  "-m",
  "PyInstaller",
  (Join-Path $Root "backend/app.py"),
  "--onefile",
  "--clean",
  "--name",
  "server",
  "--distpath",
  $OutDir,
  "--workpath",
  $BuildWorkDir,
  "--specpath",
  $BuildSpecDir
)
& $BuildPython @pyInstallerArguments
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
  Stop-PackagedBackend
}

Write-Host "[build-python] Done. Binary at $BackendExe"
