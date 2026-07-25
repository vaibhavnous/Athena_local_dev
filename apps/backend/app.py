"""Azure App Service compatibility entrypoint.

Some Python App Service configurations fall back to running ``app:app`` when a
custom startup command is not applied. Keep this thin wrapper at the package
root so both the fallback command and the explicit Procfile/startup.sh command
load the same FastAPI application.
"""

from api.main import app

