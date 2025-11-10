from pydantic import BaseModel, Field

class VersionInfo(BaseModel):
    name: str = "astro-reason-api"
    version: str = "0.1.0"

class IngestResponse(BaseModel):
    job_id: str
    object_uri: str = Field(description="s3://bucket/key style URI")
