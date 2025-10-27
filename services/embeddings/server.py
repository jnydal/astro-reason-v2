from fastapi import FastAPI
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer

class EmbedRequest(BaseModel):
    texts: list[str]

class EmbedResponse(BaseModel):
    model: str
    vectors: list[list[float]]
    dim: int

app = FastAPI()
model_name = "BAAI/bge-small-en-v1.5"
model = SentenceTransformer(model_name)

@app.get("/healthz")
def healthz():
    return {"status": "ok", "model": model_name}

@app.post("/embed", response_model=EmbedResponse)
def embed(req: EmbedRequest):
    vecs = model.encode(req.texts, normalize_embeddings=True).tolist()
    dim = len(vecs[0]) if vecs else 0
    return EmbedResponse(model=model_name, vectors=vecs, dim=dim)
