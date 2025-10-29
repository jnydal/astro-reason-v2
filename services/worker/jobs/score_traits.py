import os, json, httpx

SYSTEM_PROMPT = (
    "You analyze biographies using Yuri Burlan's System-Vector Psychology. "
    "Score each of the 8 vectors on a 1..7 scale based ONLY on the biography content. "
    "If evidence is weak, use 4 and state 'insufficient evidence' in rationale. "
    "Return strict JSON that matches the provided schema. No extra text."
)

VECTORS = [
    "sound",      # abstract thinking, introversion, meaning-seeking
    "visual",     # emotional expressiveness, aesthetics, empathy
    "oral",       # speech, persuasion, appetite, dependency themes
    "anal",       # order, thoroughness, perfectionism, stubbornness
    "urethral",   # leadership, risk-taking, generosity, expansion
    "skin",       # discipline, efficiency, boundaries, adaptability
    "muscular",   # willpower, practicality, endurance, action
    "olfactory"   # subtle intuition of context, survival sensing
]

def build_vector_prompt(bio_text: str) -> str:
    return f"""
Vectors to score (1..7):
- sound, visual, oral, anal, urethral, skin, muscular, olfactory

Scoring rules:
- Base scores only on the biography below. Do not use outside knowledge.
- If evidence is unclear for a vector, assign 4 and add rationale: "insufficient evidence".
- Identify 2-3 dominant vectors by highest scores (ties allowed).
- Provide a brief one-sentence rationale per vector citing concrete biographical cues.

Output JSON schema:
{{
  "vectors": {{
    "sound": int, "visual": int, "oral": int, "anal": int,
    "urethral": int, "skin": int, "muscular": int, "olfactory": int
  }},
  "dominant": [str],        # top 2–3 vector names by score
  "rationale": {{           # one sentence per vector
    "sound": str, "visual": str, "oral": str, "anal": str,
    "urethral": str, "skin": str, "muscular": str, "olfactory": str
  }},
  "confidence": float       # 0.0..1.0 subjective confidence from evidence quality
}}

Biography:
<<<BIO_START>>>
{bio_text}
<<<BIO_END>>>
Return only JSON.
""".strip()

def _call_chat(base, model, messages, opts):
    # Try /api/chat first
    resp = httpx.post(
        f"{base}/api/chat",
        json={"model": model, "messages": messages, "options": opts, "stream": False},
        timeout=180,
    )
    if resp.status_code == 404:
        # Fallback to /api/generate by composing a single prompt
        user_parts = [m["content"] for m in messages if m["role"] == "user"]
        prompt = (SYSTEM_PROMPT + "\n\n" + "\n\n".join(user_parts)).strip()
        resp = httpx.post(
            f"{base}/api/generate",
            json={"model": model, "prompt": prompt, "options": opts, "stream": False},
            timeout=180,
        )
        resp.raise_for_status()
        payload = resp.json()
        return payload.get("response", "")
    resp.raise_for_status()
    payload = resp.json()
    return payload.get("message", {}).get("content", "")

def score_vectors_bio(bio_text: str) -> dict:
    base = os.getenv("TRAIT_LLM_BASE_URL", "http://local-llm:11434")
    model = os.getenv("TRAIT_LLM_MODEL", "qwen2.5:7b-instruct-q4_K_M")
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": build_vector_prompt(bio_text)},
    ]
    opts = {"temperature": 0.1, "num_ctx": 4096, "repeat_penalty": 1.05}

    content = _call_chat(base, model, messages, opts)
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        # Strict retry
        messages.append({"role": "system", "content": "Your last output was not valid JSON. Return strict JSON matching the schema only."})
        content = _call_chat(base, model, messages, opts)
        return json.loads(content)
