param(
  [switch]$SkipInstall,
  [switch]$SkipDist,
  [switch]$DebugUnpacked
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $scriptDir '..\..')).Path
$packageScript = Join-Path $scriptDir 'package-desktop-win.ps1'
$desktopPackage = Join-Path $repoRoot 'apps\desktop\package.json'

Set-Location $repoRoot

if (-not $SkipInstall) {
  Write-Host '[build-desktop-local] Installing npm dependencies'
  npm ci
  if ($LASTEXITCODE -ne 0) {
    throw "npm ci failed with exit code $LASTEXITCODE"
  }
}

$version = (Get-Content -LiteralPath $desktopPackage -Raw | ConvertFrom-Json).version
$builtPackage = $false

if (-not $SkipDist) {
  & powershell -NoProfile -ExecutionPolicy Bypass -File $packageScript `
    -Version $version `
    -Target nsis `
    -SkipInstall `
    -DisableExecutableSigning
  if ($LASTEXITCODE -ne 0) {
    throw "desktop installer build failed with exit code $LASTEXITCODE"
  }
  $builtPackage = $true
}

if ($DebugUnpacked) {
  & powershell -NoProfile -ExecutionPolicy Bypass -File $packageScript `
    -Version $version `
    -Target dir `
    -SkipInstall `
    -DisableExecutableSigning
  if ($LASTEXITCODE -ne 0) {
    throw "desktop unpacked build failed with exit code $LASTEXITCODE"
  }
  $builtPackage = $true

  $unpackedDir = Join-Path $repoRoot 'apps\desktop\dist\win-unpacked'
  $desktopExe = Get-ChildItem -LiteralPath $unpackedDir -Filter '*.exe' -File |
    Where-Object { $_.Name -notlike '*unins*' } |
    Select-Object -First 1

  if (-not $desktopExe) {
    throw "No desktop executable found in $unpackedDir"
  }

  Write-Host "[build-desktop-local] Starting $($desktopExe.FullName)"
  Start-Process -FilePath $desktopExe.FullName -WorkingDirectory $unpackedDir
}

if (-not $builtPackage) {
  Write-Host '[build-desktop-local] Building backend only'
  npm run build:backend:win --workspace '@log-sparkui/desktop'
  if ($LASTEXITCODE -ne 0) {
    throw "backend build failed with exit code $LASTEXITCODE"
  }
}

Write-Host '[build-desktop-local] Done'
