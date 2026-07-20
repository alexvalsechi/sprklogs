param(
  [Parameter(Mandatory = $true)]
  [ValidatePattern('^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$')]
  [string]$Version,

  [ValidateSet('nsis', 'dir')]
  [string]$Target = 'nsis',

  [switch]$SkipInstall,

  [switch]$DisableExecutableSigning
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $scriptDir '..\..')).Path
$desktopDist = Join-Path $repoRoot 'apps\desktop\dist'
$workspaceName = '@log-sparkui/desktop'

Set-Location $repoRoot

function Invoke-Checked {
  param(
    [Parameter(Mandatory = $true)]
    [string]$Description,

    [Parameter(Mandatory = $true)]
    [scriptblock]$Command
  )

  Write-Host "[package-desktop] $Description"
  & $Command
  if ($LASTEXITCODE -ne 0) {
    throw "$Description failed with exit code $LASTEXITCODE"
  }
}

function Ensure-FromNpmTarball {
  param(
    [Parameter(Mandatory = $true)]
    [string]$PackageName,

    [Parameter(Mandatory = $true)]
    [string]$PackageVersion,

    [Parameter(Mandatory = $true)]
    [string]$FileInTar,

    [Parameter(Mandatory = $true)]
    [string]$Destination
  )

  if (Test-Path -LiteralPath $Destination) {
    Write-Host "[package-desktop] OK: $Destination"
    return
  }

  $tempRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("sprklogs-npm-pack-" + [guid]::NewGuid().ToString('N'))
  New-Item -ItemType Directory -Path $tempRoot -Force | Out-Null

  try {
    Invoke-Checked "Download $PackageName@$PackageVersion" {
      npm pack "$PackageName@$PackageVersion" --pack-destination $tempRoot
    }

    $tarball = Get-ChildItem -LiteralPath $tempRoot -Filter '*.tgz' -File | Select-Object -First 1
    if (-not $tarball) {
      throw "npm pack did not create a tarball for $PackageName@$PackageVersion"
    }

    Invoke-Checked "Extract $FileInTar" {
      tar -xzf $tarball.FullName -C $tempRoot $FileInTar
    }

    $extractedPath = Join-Path $tempRoot ($FileInTar -replace '/', '\')
    if (-not (Test-Path -LiteralPath $extractedPath)) {
      throw "Failed to extract $FileInTar from $($tarball.Name)"
    }

    New-Item -ItemType Directory -Path (Split-Path $Destination -Parent) -Force | Out-Null
    Copy-Item -LiteralPath $extractedPath -Destination $Destination -Force
  } finally {
    if (Test-Path -LiteralPath $tempRoot) {
      Remove-Item -LiteralPath $tempRoot -Recurse -Force
    }
  }
}

if ([System.Environment]::OSVersion.Platform -ne [System.PlatformID]::Win32NT) {
  throw 'Desktop packaging is supported only on Windows.'
}

if (-not $SkipInstall) {
  Invoke-Checked 'Install npm dependencies' { npm ci }
}

Invoke-Checked "Set desktop version to $Version" {
  npm version $Version --allow-same-version --no-git-tag-version --workspace $workspaceName
}

Write-Host '[package-desktop] Ensuring electron-builder native binaries'
Ensure-FromNpmTarball `
  -PackageName '7zip-bin' `
  -PackageVersion '5.2.0' `
  -FileInTar 'package/win/x64/7za.exe' `
  -Destination (Join-Path $repoRoot 'node_modules\7zip-bin\win\x64\7za.exe')
Ensure-FromNpmTarball `
  -PackageName 'app-builder-bin' `
  -PackageVersion '4.0.0' `
  -FileInTar 'package/win/x64/app-builder.exe' `
  -Destination (Join-Path $repoRoot 'node_modules\app-builder-bin\win\x64\app-builder.exe')

# Older electron-builder dependencies occasionally omit their bundled Windows
# executables. Keep the workaround in one place until electron-builder is upgraded.
$sevenZipIndex = Join-Path $repoRoot 'node_modules\7zip-bin\index.js'
if (Test-Path -LiteralPath $sevenZipIndex) {
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
    '    if (fs.existsSync(bundled)) return bundled'
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
  Set-Content -LiteralPath $sevenZipIndex -Value $indexContent -Encoding utf8 -Force
}

$builder7za = Join-Path $repoRoot 'node_modules\builder-util\out\7za.js'
if (Test-Path -LiteralPath $builder7za) {
  $builderContent = @(
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
  Set-Content -LiteralPath $builder7za -Value $builderContent -Encoding utf8 -Force
}

$npmScript = if ($Target -eq 'nsis') {
  if ($DisableExecutableSigning) { 'dist:win:local' } else { 'dist:win' }
} else {
  if ($DisableExecutableSigning) { 'pack:win:debug' } else { 'pack:win' }
}
Invoke-Checked "Build desktop target $Target" {
  npm run $npmScript --workspace $workspaceName
}

$packagedBackend = Join-Path $desktopDist 'win-unpacked\resources\backend\server.exe'
if (-not (Test-Path -LiteralPath $packagedBackend)) {
  throw "Missing packaged backend executable: $packagedBackend"
}

$pythonSources = Get-ChildItem `
  -Path (Join-Path $desktopDist 'win-unpacked\resources\backend') `
  -Recurse `
  -Filter '*.py' `
  -File `
  -ErrorAction SilentlyContinue
if ($pythonSources) {
  $listedSources = ($pythonSources | ForEach-Object { $_.FullName }) -join "`n"
  throw "Packaged backend contains Python source files:`n$listedSources"
}

if ($Target -eq 'nsis') {
  $installer = Join-Path $desktopDist "SprkLogs-setup-v$Version.exe"
  if (-not (Test-Path -LiteralPath $installer)) {
    throw "Missing expected installer: $installer"
  }

  $hash = Get-FileHash -LiteralPath $installer -Algorithm SHA256
  $checksumLines = "$($hash.Hash)  $($installer | Split-Path -Leaf)"
  $checksumPath = Join-Path $desktopDist 'checksums.txt'
  Set-Content -LiteralPath $checksumPath -Value $checksumLines -Encoding utf8
  Write-Host "[package-desktop] Checksums written to $checksumPath"
}

Write-Host "[package-desktop] Desktop $Target build v$Version completed"
