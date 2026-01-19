# FastAPI wrapper for fetch_bio.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os
import sys
from .fetch_bio import run

app = FastAPI(title="Fetch Bio API", version="0.1.0")

class FetchBioRequest(BaseModel):
    lang: str = "en"
    limit: int = 500

class FetchBioResponse(BaseModel):
    status: str
    written: int
    message: str

@app.get("/healthz")
def healthz():
    return {"status": "ok"}

@app.post("/fetch-bio", response_model=FetchBioResponse)
def fetch_bio_endpoint(request: FetchBioRequest = FetchBioRequest()):
    """
    Fetch Wikipedia biographies for people with QIDs but no text.
    """
    dsn = os.getenv("PG_DSN") or os.getenv("DATABASE_URL")
    if not dsn:
        raise HTTPException(status_code=500, detail="PG_DSN or DATABASE_URL must be set")
    
    try:
        # Run fetch_bio and get the count of written records
        written = run(dsn, request.lang, request.limit)
        
        return FetchBioResponse(
            status="ok",
            written=written,
            message=f"Fetched {written} biographies"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching bios: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8002"))
    uvicorn.run(app, host="0.0.0.0", port=port)
