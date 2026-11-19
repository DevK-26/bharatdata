"""BharatData public API — foundations only.

Tonight this service exposes ``/health`` and nothing else: dataset and row
endpoints arrive in Phase C of BUILD-SPRINT.md. Do not add placeholder routes.
"""

from fastapi import FastAPI

from bharatdata import __version__

app = FastAPI(
    title="BharatData API",
    version=__version__,
    description=(
        "Cleaned Government of India open data. Not a Government of India "
        "product; data sourced from the OGD Platform India under GODL."
    ),
)


@app.get("/health", tags=["meta"])
def health() -> dict[str, str]:
    """Liveness probe used by Docker and by uptime checks."""
    return {"status": "ok", "service": "bharatdata-api", "version": __version__}
