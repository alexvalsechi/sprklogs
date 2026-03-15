param(
  [switch]$SkipInstall,
  [switch]$SkipDist
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir "..\..")
Set-Location $repoRoot

Write-Host "Repo root: $repoRoot"

if (-not $SkipInstall) {
  Write-Host "Running npm ci..."
  npm ci
}

function Ensure-FromNpmTarball {
  param(
    [string]$PackageName,
    [string]$Version,
    [string]$FileInTar,
    [string]$Destination
  )

  if (Test-Path $Destination) {
    Write-Host "OK: already exists $Destination"
    return
  }

  $tgz = "$PackageName-$Version.tgz"
  Write-Host "Downloading $PackageName@$Version via npm pack..."
  npm pack "$PackageName@$Version"

  if (-not (Test-Path $tgz)) {
    throw "Failed to download $tgz"
  }

  tar -xzf $tgz "$FileInTar"
  $extractedPath = $FileInTar -replace '^package/', 'package\\'
  if (-not (Test-Path $extractedPath)) {
    throw "Failed to extract $FileInTar from $tgz"
  }

  New-Item -ItemType Directory -Path (Split-Path $Destination -Parent) -Force | Out-Null
  Copy-Item $extractedPath $Destination -Force

  Remove-Item $tgz -Force -ErrorAction SilentlyContinue
  Remove-Item package -Recurse -Force -ErrorAction SilentlyContinue
  Write-Host "OK: restored $Destination"
}

Write-Host "Ensuring electron-builder native binaries..."
Ensure-FromNpmTarball -PackageName "7zip-bin" -Version "5.2.0" -FileInTar "package/win/x64/7za.exe" -Destination "node_modules/7zip-bin/win/x64/7za.exe"
Ensure-FromNpmTarball -PackageName "app-builder-bin" -Version "4.0.0" -FileInTar "package/win/x64/app-builder.exe" -Destination "node_modules/app-builder-bin/win/x64/app-builder.exe"

$sevenZipIndex = "node_modules/7zip-bin/index.js"
if (Test-Path $sevenZipIndex) {
  $indexContent = @(
    '"use strict"'
    'const path = require("path")'
    'const fs = require("fs")'
    ''
    'function getPath() {'
    '  if (process.platform === "darwin") {'
    '    const p = path.join(__dirname, "mac", process.arch, "7za")'
    '    return fs.existsSync(p) ? p : "7za"'
    '  }'
    '  if (process.platform === "win32") {'
    '    const bundled = path.join(__dirname, "win", process.arch, "7za.exe")'
    '    if (fs.existsSync(bundled)) {'
    '      return bundled'
    '    }'
    '    const system7z = "C:\\Program Files\\7-Zip\\7z.exe"'
    '    return fs.existsSync(system7z) ? system7z : bundled'
    '  }'
    '  const p = path.join(__dirname, "linux", process.arch, "7za")'
    '  return fs.existsSync(p) ? p : "7za"'
    '}'
    ''
    'exports.path7za = getPath()'
    'exports.path7x = path.join(__dirname, "7x.sh")'
  ) -join "`n"
  Set-Content -Path $sevenZipIndex -Value $indexContent -Encoding utf8 -Force
}

$builder7za = "node_modules/builder-util/out/7za.js"
if (Test-Path $builder7za) {
  $patchedBuilder7za = @(
    '"use strict";'
    'Object.defineProperty(exports, "__esModule", { value: true });'
    'exports.getPath7x = exports.getPath7za = void 0;'
    'const _7zip_bin_1 = require("7zip-bin");'
    'const fs = require("fs");'
    'const fs_extra_1 = require("fs-extra");'
    'async function getPath7za() {'
    '    if (fs.existsSync(_7zip_bin_1.path7za)) {'
    '        await (0, fs_extra_1.chmod)(_7zip_bin_1.path7za, 0o755);'
    '    }'
    '    return _7zip_bin_1.path7za;'
    '}'
    'exports.getPath7za = getPath7za;'
    'async function getPath7x() {'
    '    if (fs.existsSync(_7zip_bin_1.path7x)) {'
    '        await (0, fs_extra_1.chmod)(_7zip_bin_1.path7x, 0o755);'
    '    }'
    '    return _7zip_bin_1.path7x;'
    '}'
    'exports.getPath7x = getPath7x;'
  ) -join "`n"
  Set-Content -Path $builder7za -Value $patchedBuilder7za -Encoding utf8 -Force
}

Write-Host "Verifying paths..."
node -e "const fs=require('fs'); const p7='node_modules/7zip-bin/win/x64/7za.exe'; const pab='node_modules/app-builder-bin/win/x64/app-builder.exe'; console.log('7za path:', p7, 'exists:', fs.existsSync(p7)); console.log('app-builder path:', pab, 'exists:', fs.existsSync(pab));"

if (-not $SkipDist) {
  Write-Host "Running desktop dist build..."
  npm run dist:win --workspace @log-sparkui/desktop
}

Write-Host "Done."
