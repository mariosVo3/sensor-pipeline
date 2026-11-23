from fastapi import FastAPI

# The app version lives here, in one place
VERSION = "0.1.0"

app = FastAPI(title="Sensor Pipeline")


@app.get("/health")
def health() -> dict[str, str]:
    """Simple check that the API is running."""
    return {"status": "ok"}


@app.get("/version")
def version() -> dict[str, str]:
    """Return the app version."""
    return {"version": VERSION}
