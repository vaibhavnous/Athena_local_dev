# Azure deployment

Azure deployment assets live together here:

```text
devops/azure/
├── pipelines/
│   ├── backend.yml
│   └── frontend.yml
├── infrastructure/
│   ├── main.bicep
│   └── main.parameters.json
├── scripts/
│   └── deploy-setup.sh
├── ENVIRONMENT.md
└── README.md
```

## Deployment entry points

Production GitHub Actions remain under `.github/workflows/` because GitHub only discovers workflows there.

The root `azure-pipelines-backend.yml` and `azure-pipelines-frontend.yml` files are compatibility entry points for existing Azure DevOps pipelines. They retain triggers and extend the canonical stage templates in this directory. Keep Azure DevOps configured to the root files so this reorganization does not require an external pipeline-path change.

## Infrastructure

`infrastructure/main.bicep` provisions the optional combined App Service development stack. It does not replace the current production GitHub Actions workflows, which deploy the existing backend App Service and frontend Static Web App.

The tracked parameter file contains non-secret defaults only. Supply the required SQL values through the setup script or secure deployment variables; never commit credentials.

Validate the template without deploying:

```bash
az bicep build --file devops/azure/infrastructure/main.bicep
```

Deploy the development stack from the repository root:

```bash
bash devops/azure/scripts/deploy-setup.sh
```

The script accepts these optional environment variables:

- `PROJECT_NAME`
- `ENVIRONMENT`
- `LOCATION`
- `RESOURCE_GROUP`
- `ATHENA_SQL_HOST`
- `ATHENA_SQL_USERNAME`
- `ATHENA_SQL_PASSWORD`

Unset SQL values are requested interactively. See `ENVIRONMENT.md` for application settings and GitHub deployment secrets.
