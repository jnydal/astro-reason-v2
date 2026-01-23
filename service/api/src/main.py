import io
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from tenacity import retry, stop_after_attempt, wait_fixed
from .schemas import VersionInfo, IngestResponse
from . import storage, jobs

app = FastAPI(title="Astro Reason API", version="0.1.0")

@app.on_event("startup")
def _startup():
    storage.ensure_bucket()

@app.get("/healthz")
def healthz():
    return {"status": "ok"}

@app.get("/version", response_model=VersionInfo)
def version():
    return VersionInfo()

@retry(stop=stop_after_attempt(2), wait=wait_fixed(0.2))
def _read_upload(file: UploadFile) -> bytes:
    data = file.file.read()
    if not data or len(data) == 0:
        raise ValueError("empty upload")
    return data

@app.post("/ingest/astrodatabank", response_model=IngestResponse)
async def ingest_astrodatabank(xml: UploadFile = File(...)):
    if "xml" not in (xml.content_type or "") and not (xml.filename or "").lower().endswith(".xml"):
        raise HTTPException(400, detail="Expected an .xml file")

    try:
        content = _read_upload(xml)
    except Exception as e:
        raise HTTPException(400, detail=f"Could not read file: {e}")

    # (Optional) very light sanity check: does it look like the adb export?
    if b"<astrodatabank" not in content and b"<AstroDatabank" not in content:
        # allow anyway, worker can fail with better diagnostics
        pass

    object_uri = storage.put_bytes("adb-uploads", content, content_type="application/xml")
    job = jobs.enqueue_parse_adb_xml(object_uri, source_label="astrodb-upload")
    return IngestResponse(job_id=job["id"], object_uri=object_uri)

@app.get("/jobs/{job_id}")
def job_status(job_id: str):
    # lightweight polling endpoint (optional but handy)
    job = jobs.fetch_job_status(job_id)
    if not job:
        raise HTTPException(404, detail="job not found")

    return JSONResponse({
        "id": str(job["id"]),
        "status": job["status"].lower() if job["status"] else "unknown",
        "enqueued_at": str(job["enqueued_at"]) if job.get("enqueued_at") else None,
        "started_at": str(job["started_at"]) if job.get("started_at") else None,
        "ended_at": str(job["ended_at"]) if job.get("ended_at") else None,
        "exc_info": job.get("exc_info"),
        "result": job.get("result"),
    })
