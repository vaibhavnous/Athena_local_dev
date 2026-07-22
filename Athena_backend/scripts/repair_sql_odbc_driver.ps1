<#
Run from an elevated PowerShell window.

Check only:
  .\repair_sql_odbc_driver.ps1

Uninstall Microsoft SQL ODBC 17/18, reinstall Driver 18, then verify pyodbc:
  .\repair_sql_odbc_driver.ps1 -Repair

Offline MSI install:
  .\repair_sql_odbc_driver.ps1 -Repair -InstallerPath C:\Temp\msodbcsql18.msi
#>

param(
    [switch]$Repair,
    [string]$InstallerPath,
    [switch]$SkipPythonVerify
)

$ErrorActionPreference = "Stop"

function Assert-Admin {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = [Security.Principal.WindowsPrincipal]::new($identity)
    if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        throw "Run this script from an elevated PowerShell window."
    }
}

function Get-SqlOdbcUninstallEntries {
    $paths = @(
        "HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\*",
        "HKLM:\SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall\*"
    )

    Get-ItemProperty -Path $paths -ErrorAction SilentlyContinue |
        Where-Object { $_.DisplayName -match "^Microsoft ODBC Driver (17|18) for SQL Server" } |
        Sort-Object DisplayName, DisplayVersion -Unique
}

function Show-InstalledState {
    Write-Host "Installed Microsoft SQL ODBC drivers:"
    $drivers = Get-SqlOdbcUninstallEntries
    if ($drivers) {
        $drivers | Select-Object DisplayName, DisplayVersion, PSChildName | Format-Table -AutoSize
    } else {
        Write-Host "  None found through uninstall registry."
    }

    Write-Host ""
    Write-Host "ODBC registry driver names:"
    $odbcDrivers = Get-ItemProperty "HKLM:\SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers" -ErrorAction SilentlyContinue
    if ($odbcDrivers) {
        $odbcDrivers.PSObject.Properties |
            Where-Object { $_.Name -like "*SQL Server*" } |
            Select-Object Name, Value |
            Format-Table -AutoSize
    } else {
        Write-Host "  Could not read ODBC driver registry."
    }
}

function Uninstall-SqlOdbcDrivers {
    $drivers = Get-SqlOdbcUninstallEntries
    foreach ($driver in $drivers) {
        Write-Host "Uninstalling $($driver.DisplayName) $($driver.DisplayVersion)"
        $productCode = $driver.PSChildName
        if ($productCode -notmatch "^\{[0-9A-Fa-f-]{36}\}$") {
            throw "Cannot uninstall $($driver.DisplayName): registry product code is not an MSI GUID."
        }

        $process = Start-Process msiexec.exe -ArgumentList "/x", $productCode, "/quiet", "/norestart" -Wait -PassThru
        if ($process.ExitCode -notin @(0, 3010, 1605)) {
            throw "Uninstall failed for $($driver.DisplayName). msiexec exit code: $($process.ExitCode)"
        }
    }
}

function Install-SqlOdbcDriver18 {
    if ($InstallerPath) {
        if (-not (Test-Path -LiteralPath $InstallerPath)) {
            throw "InstallerPath not found: $InstallerPath"
        }

        Write-Host "Installing Driver 18 from local MSI: $InstallerPath"
        $process = Start-Process msiexec.exe -ArgumentList "/i", $InstallerPath, "/quiet", "/norestart", "IACCEPTMSODBCSQLLICENSETERMS=YES" -Wait -PassThru
        if ($process.ExitCode -notin @(0, 3010)) {
            throw "Install failed. msiexec exit code: $($process.ExitCode)"
        }
        return
    }

    $winget = Get-Command winget.exe -ErrorAction SilentlyContinue
    if (-not $winget) {
        throw "winget is not available. Re-run with -InstallerPath pointing to the official msodbcsql18.msi."
    }

    Write-Host "Installing Microsoft ODBC Driver 18 for SQL Server through winget"
    $process = Start-Process winget.exe -ArgumentList @(
        "install",
        "--id", "Microsoft.msodbcsql.18",
        "--exact",
        "--silent",
        "--accept-package-agreements",
        "--accept-source-agreements"
    ) -Wait -PassThru
    if ($process.ExitCode -ne 0) {
        throw "winget install failed. Exit code: $($process.ExitCode)"
    }
}

function Test-PythonPyodbc {
    $python = Get-Command python.exe -ErrorAction SilentlyContinue
    if (-not $python) {
        Write-Warning "python.exe not found on PATH; skipping pyodbc verification."
        return
    }

    $code = @"
import pyodbc
drivers = pyodbc.drivers()
print("pyodbc drivers:", drivers)
raise SystemExit(0 if "ODBC Driver 18 for SQL Server" in drivers else 1)
"@
    $code | python -
    if ($LASTEXITCODE -ne 0) {
        throw "pyodbc cannot see 'ODBC Driver 18 for SQL Server' after install."
    }
}

Assert-Admin
Show-InstalledState

if (-not $Repair) {
    Write-Host ""
    Write-Host "Check complete. Re-run with -Repair to uninstall/reinstall Microsoft ODBC Driver 18."
    exit 0
}

Write-Host ""
Uninstall-SqlOdbcDrivers
Install-SqlOdbcDriver18

Write-Host ""
Show-InstalledState

if (-not $SkipPythonVerify) {
    Write-Host ""
    Test-PythonPyodbc
}

Write-Host ""
Write-Host "ODBC repair complete. Restart Windows or at least restart the backend process before testing SQL again."
