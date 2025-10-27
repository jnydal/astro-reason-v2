import os, json, httpx

SYSTEM_PROMPT = (
    "You extract personality traits from biographies using a fixed rubric. "
    "Return strict JSON that matches the provided schema. No extra text."
)

def build_trait_prompt(bio_text: str) -> str:
    return f"""
Rubric (Big Five 1..7):
- openness, conscientiousness, extraversion, agreeableness, neuroticism
Rules:
- Base scores only on the biography.
- If uncertain, use 4 and say "insufficient evidence".
Schema:
{{
  "big5": {{
    "openness": int, "conscientiousness": int, "extraversion": int,
    "agreeableness": int, "neuroticism": int
  }},
  "facets": {{
    "orderliness": int|null, "industriousness": int|null, "assertiveness": int|null,
    "enthusiasm": int|null, "compassion": int|null, "politeness": int|null,
    "intellect": int|null, "artistic": int|null, "volatility": int|null, "withdrawal": int|null
  }},
  "rationale": {{
    "openness": str, "conscientiousness": str, "extraversion": str,
    "agreeableness": str, "neuroticism": str
  }}
}}
Biography:
<<<BIO_START>>>
{bio_text}
<<<BIO_END>>>
Return only JSON.
""".strip()

def score_traits_bio(bio_text: str) -> dict:
    base = os.getenv("TRAIT_LLM_BASE_URL", "http://local-llm:11434")
    model = os.getenv("TRAIT_LLM_MODEL", "qwen2.5:7b-instruct-q4_K_M")
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": build_trait_prompt(bio_text)},
    ]
    opts = {
        "temperature": 0.1,      # better JSON discipline
        "num_ctx": 4096,         # context length
        "repeat_penalty": 1.05,  # tame repetition
    }

    def call(messages):
        r = httpx.post(f"{base}/api/chat", json={"model": model, "messages": messages, "options": opts, "stream": False}, timeout=180)
        r.raise_for_status()
        return r.json()["message"]["content"]

    content = call(messages)
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        messages.append({"role": "system", "content": "Your last output was not valid JSON. Return strict JSON matching the schema only."})
        content = call(messages)
        return json.loads(content)
