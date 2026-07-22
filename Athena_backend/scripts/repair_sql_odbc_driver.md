# Repair Microsoft ODBC Driver 18 for SQL Server

This runbook is for IT/admin support. The local app is failing before SQL login with:

```text
ODBC Driver 18 for SQL Server
Encryption not supported on the client
SSL Provider: No credentials are available in the security package
```

That points to the local Microsoft ODBC/TLS client installation, not the app password or Azure SQL firewall.

## Goal

Remove the broken Microsoft SQL ODBC driver installation and reinstall the correct 64-bit Microsoft ODBC Driver 18 for SQL Server.

## Required Access

- Windows administrator rights
- PowerShell or Windows Terminal opened as Administrator
- Internet access, or an offline copy of the official Microsoft `msodbcsql18.msi` installer

## 1. Check Current Driver State

Open PowerShell as Administrator and run:

```powershell
python -c "import pyodbc; print(pyodbc.version); print(pyodbc.drivers())"
```

Also check installed ODBC packages:

```powershell
Get-ItemProperty `
  HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\*, `
  HKLM:\SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall\* |
  Where-Object { $_.DisplayName -match '^Microsoft ODBC Driver (17|18) for SQL Server' } |
  Select-Object DisplayName, DisplayVersion, PSChildName
```

Expected broken state on this machine:

```text
ODBC Driver 18 for SQL Server is visible, but SQL login fails with TLS/security-package error.
```

## 2. Uninstall Existing Microsoft SQL ODBC Drivers

Go to:

```text
Windows Settings > Apps > Installed apps
```

Uninstall these if present:

```text
Microsoft ODBC Driver 18 for SQL Server
Microsoft ODBC Driver 17 for SQL Server
```

If Settings does not remove them cleanly, use Control Panel:

```text
Control Panel > Programs > Programs and Features
```

Then uninstall the same Microsoft ODBC Driver entries.

## 3. Reboot Windows

Restart the machine after uninstalling the driver.

This matters because the failure is in the local TLS/security provider path used by the ODBC driver.

## 4. Install Correct Driver

Install the official Microsoft ODBC Driver 18 for SQL Server, 64-bit.

Preferred IT route:

```text
Microsoft ODBC Driver 18 for SQL Server
Architecture: x64
Package: msodbcsql18.msi
```

If `winget` is allowed, IT can install with:

```powershell
winget install --id Microsoft.msodbcsql.18 --exact --silent --accept-package-agreements --accept-source-agreements
```

If using the MSI manually, accept the Microsoft license terms and install the x64 driver.

## 5. Verify Driver Is Visible To Python

After install, open a new PowerShell window and run:

```powershell
python -c "import pyodbc; print(pyodbc.version); print(pyodbc.drivers())"
```

Expected output must include:

```text
ODBC Driver 18 for SQL Server
```

## 6. Verify The App Connection

From the repo root:

```powershell
cd C:\Users\vaibhavmalik\athena_localdev
```

Run:

```powershell
$code = @'
import sys
sys.path.insert(0, "Athena_backend")
from utilis.db import get_pipeline_connection

conn = get_pipeline_connection()
print("PIPELINE_SQL_OK")
conn.close()
'@
$code | python -
```

Expected success:

```text
PIPELINE_SQL_OK
```

## 7. If It Still Fails

Send the full output of these two commands back to the developer:

```powershell
python -c "import pyodbc; print(pyodbc.version); print(pyodbc.drivers())"
```

```powershell
$code = @'
import sys
sys.path.insert(0, "Athena_backend")
from utilis.db import get_pipeline_connection

try:
    conn = get_pipeline_connection()
    print("PIPELINE_SQL_OK")
    conn.close()
except Exception as exc:
    print(type(exc).__name__)
    print(str(exc))
'@
$code | python -
```

Do not change app credentials or Azure SQL firewall rules unless the error changes away from the ODBC TLS/security-package message.
