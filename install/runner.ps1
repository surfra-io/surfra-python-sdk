# Installs the surfra command on Windows. A local runner also needs Docker Desktop.
#   irm https://surfra.io/runner.ps1 | iex
# SURFRA_HOME holds the Python environment (default %USERPROFILE%\.surfra). SURFRA_PACKAGE installs a different wheel.
$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

$repo = "surfra-io/surfra-python-sdk"
$surfraHome = if ($env:SURFRA_HOME) { $env:SURFRA_HOME } else { Join-Path $env:USERPROFILE ".surfra" }
$venv = Join-Path $surfraHome "venv"
$scripts = Join-Path $venv "Scripts"

$package = $env:SURFRA_PACKAGE
if (-not $package) {
    $release = Invoke-RestMethod "https://api.github.com/repos/$repo/releases/latest" -Headers @{ "User-Agent" = "surfra-installer" }
    $wheel = $release.assets | Where-Object { $_.name -like "*.whl" } | Select-Object -First 1
    if (-not $wheel) { throw "surfra: the latest release has no wheel" }
    $package = $wheel.browser_download_url
}

function Find-Python {
    foreach ($candidate in @(@("py", "-3.13"), @("py", "-3.12"), @("py", "-3.11"), @("python"))) {
        $exe = $candidate[0]
        if (-not (Get-Command $exe -ErrorAction SilentlyContinue)) { continue }
        $check = @($candidate | Select-Object -Skip 1) + @("-c", "import sys; sys.exit(sys.version_info < (3, 11))")
        & $exe @check 2>$null
        if ($LASTEXITCODE -eq 0) { return , $candidate }
    }
    return $null
}

New-Item -ItemType Directory -Force -Path $surfraHome | Out-Null
$venvPython = Join-Path $scripts "python.exe"
$uvExe = Join-Path (Join-Path $surfraHome "bin") "uv.exe"
$upgraded = $false
if (Test-Path $venvPython) {
    # Upgrade in place: a running surfra.exe locks the venv, so it cannot be deleted.
    & $venvPython -m pip --version 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) {
        & $venvPython -m pip install --quiet --upgrade $package
        $upgraded = $true
    } elseif (Test-Path $uvExe) {
        & $uvExe pip install --quiet --python $venvPython $package
        $upgraded = $true
    }
}
if (-not $upgraded -and (Test-Path $venv)) { Remove-Item -Recurse -Force $venv }
$python = if ($upgraded) { $null } else { Find-Python }
if ($upgraded) {
} elseif ($python) {
    $exe = $python[0]
    $rest = @($python | Select-Object -Skip 1)
    & $exe @rest -m venv $venv
    & (Join-Path $scripts "python.exe") -m pip install --quiet --upgrade pip
    & (Join-Path $scripts "python.exe") -m pip install --quiet --upgrade $package
} else {
    Write-Host "Python 3.11 or newer was not found. Installing a private copy with uv."
    $env:UV_INSTALL_DIR = Join-Path $surfraHome "bin"
    $env:UV_NO_MODIFY_PATH = "1"
    Invoke-RestMethod https://astral.sh/uv/install.ps1 | Invoke-Expression
    $uv = Join-Path $env:UV_INSTALL_DIR "uv.exe"
    & $uv venv --quiet --python 3.12 $venv
    & $uv pip install --quiet --python (Join-Path $scripts "python.exe") $package
}
if ($LASTEXITCODE -ne 0) { throw "surfra: install failed" }

$userPath = [Environment]::GetEnvironmentVariable("Path", "User")
if (-not ($userPath -split ";" | Where-Object { $_ -eq $scripts })) {
    [Environment]::SetEnvironmentVariable("Path", "$scripts;$userPath", "User")
    $env:Path = "$scripts;$env:Path"
}
Write-Host "surfra installed to $scripts\surfra.exe. Open a new terminal so PATH picks it up."
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Host "For a local runner, install Docker Desktop: https://www.docker.com/products/docker-desktop/"
}
Write-Host ""
Write-Host "Next:"
Write-Host "  surfra login                 # or `$env:SURFRA_API_KEY = 'sfk_...'"
Write-Host "  surfra runner register       # add --name my-box to skip the browser"
Write-Host "  surfra runner start"
