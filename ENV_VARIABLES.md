# Environment Variables Configuration

## Backend App Service (Python)

### From Key Vault (Automatic)
```
SQL_CONNECTION_STRING=@Microsoft.KeyVault(VaultName=athena-kv-dev;SecretName=sql-connection-string)
API_KEY=@Microsoft.KeyVault(VaultName=athena-kv-dev;SecretName=api-key)
```

### Application Configuration
```
ENVIRONMENT=dev
PYTHONUNBUFFERED=1
LOG_LEVEL=INFO
```

### Application Insights
```
APPINSIGHTS_INSTRUMENTATIONKEY={generated from Bicep}
APPLICATIONINSIGHTS_CONNECTION_STRING=InstrumentationKey={generated from Bicep}
```

### App Service Specific
```
WEBSITES_ENABLE_APP_SERVICE_STORAGE=false
```

---

## Frontend App Service (Node.js / React)

### API Configuration
```
REACT_APP_API_BASE_URL=https://athena-backend-dev.azurewebsites.net
REACT_APP_ENVIRONMENT=dev
```

### Application Insights
```
APPINSIGHTS_INSTRUMENTATIONKEY={generated from Bicep}
```

### App Service Specific
```
WEBSITES_ENABLE_APP_SERVICE_STORAGE=false
```

---

## Azure DevOps Pipeline Variables

### Required Variable Group: `athena-secrets`
```
AZURE_SUBSCRIPTION_ID=<your-subscription-id>
AZURE_DEVOPS_SERVICE_CONNECTION=azure-devops-service-connection
SQL_CONNECTION_STRING=<database-connection-string>
API_KEY=<your-api-key>
```

---

## Key Vault Secrets

The Bicep template creates these secrets automatically:

### sql-connection-string
```
Value: Server={sql_server};Database={db};User Id={user};Password={password};
```

### api-key
```
Value: {your-api-key-value}
```

---

## Local Development Variables

### Backend (.env or environment)
```
ENVIRONMENT=local
SQL_CONNECTION_STRING=Server=localhost;Database=athena;User Id=sa;Password=YourPassword123;
API_KEY=dev-key-12345
LOG_LEVEL=DEBUG
DATABASE_URL=mssql://sa:YourPassword123@localhost/athena
```

### Frontend (.env)
```
REACT_APP_API_ENDPOINT=http://localhost:8000
REACT_APP_ENVIRONMENT=local
```

---

## Configuration Priority

### App Service Resolution Order
1. **App Settings** (highest priority)
2. **Connection Strings**
3. **Key Vault references** (if configured)
4. **Environment defaults** (lowest priority)

### Key Vault Reference Syntax
```
@Microsoft.KeyVault(VaultName=<vault-name>;SecretName=<secret-name>)
```

When configured, App Service automatically fetches from Key Vault at runtime.

---

## Setting Variables in App Service

### Using Azure CLI
```bash
az webapp config appsettings set \
  --resource-group athena-rg-dev \
  --name athena-backend-dev \
  --settings \
    KEY1=value1 \
    KEY2=value2 \
    KEY3="@Microsoft.KeyVault(VaultName=athena-kv-dev;SecretName=key3)"
```

### Using Azure Portal
1. Go to App Service → Configuration
2. Add new application setting
3. For Key Vault references, use syntax: `@Microsoft.KeyVault(VaultName=...;SecretName=...)`
4. Click Save

---

## Connection String Specific Configuration

### SQL Server Connection String Format
```
Server=<server>.database.windows.net;Database=<db>;User Id=<user>@<server>;Password=<password>;Encrypt=true;Connection Timeout=30;
```

### For Key Vault Reference
1. Create secret in Key Vault with the full connection string
2. Reference it in App Service as: `@Microsoft.KeyVault(VaultName=..;SecretName=sql-connection-string)`

---

## Sensitive Variables Best Practices

✓ **DO:**
- Store all secrets in Key Vault
- Use managed identities for access
- Reference secrets in App Service config, not hardcoded
- Rotate secrets regularly
- Enable Key Vault soft delete

✗ **DON'T:**
- Store secrets in code or version control
- Use hardcoded API keys
- Share secrets via email
- Commit .env files with real values
- Use the same key across environments

---

## Verifying Configuration

### Check Backend App Settings
```bash
az webapp config appsettings list \
  --resource-group athena-rg-dev \
  --name athena-backend-dev
```

### Check Frontend App Settings
```bash
az webapp config appsettings list \
  --resource-group athena-rg-dev \
  --name athena-frontend-dev
```

### View App Service Logs
```bash
az webapp log tail \
  --resource-group athena-rg-dev \
  --name athena-backend-dev
```

### Test Key Vault Access
```bash
az keyvault secret show \
  --vault-name athena-kv-dev \
  --name sql-connection-string
```
