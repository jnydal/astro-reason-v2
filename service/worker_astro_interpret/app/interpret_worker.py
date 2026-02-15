"""
Astro interpreter worker.

Consumes from the astro Kafka topic (consumer group: astro-interpreter).
Handles messages with function "astro.interpret"; loads astro_features for the person,
calls the LLM to produce an astrological reading, and stores the result in astro_interpretations.

Run:
    python -m service.worker_astro_interpret.app.interpret_worker
"""
from __future__ import annotations

import json
import os
import time

import psycopg2
import psycopg2.extras
import requests
from confluent_kafka import Consumer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "astro-interpreter")
KAFKA_TOPIC = os.getenv("KAFKA_ASTRO_TOPIC", "astro")
OLLAMA_URL = (os.getenv("OLLAMA_URL") or "http://local-llm:11434").rstrip("/")
LLM_MODEL = os.getenv("LLM_MODEL", "qwen2.5:7b-instruct-q4_K_M")


def _ollama_timeout() -> int:
    raw = os.getenv("OLLAMA_TIMEOUT", "300")
    try:
        return max(60, int(raw))
    except ValueError:
        return 300


def _normalize_dsn(dsn: str) -> str:
    if dsn.startswith("postgresql+psycopg://"):
        return "postgresql://" + dsn[len("postgresql+psycopg://") :]
    if dsn.startswith("postgresql+psycopg2://"):
        return "postgresql://" + dsn[len("postgresql+psycopg2://") :]
    return dsn


def _get_dsn() -> str:
    raw = os.getenv("PG_DSN") or os.getenv("DATABASE_URL") or ""
    return _normalize_dsn(raw)


def _has_interpretation(cur, person_id: str) -> bool:
    cur.execute(
        "SELECT 1 FROM astro_interpretations WHERE person_id = %s LIMIT 1",
        (person_id,),
    )
    return cur.fetchone() is not None


def _load_chart(cur, person_id: str) -> dict | None:
    cur.execute(
        """
        SELECT longs, houses, aspects, elem_ratios, modality_ratios
        FROM astro_features
        WHERE person_id = %s
        """,
        (person_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return dict(row)


def _format_chart_for_prompt(chart: dict) -> str:
    parts = []
    if chart.get("longs"):
        parts.append("Planetary longitudes (ecliptic deg): " + json.dumps(chart["longs"]))
    if chart.get("houses"):
        parts.append("Houses: " + json.dumps(chart["houses"]))
    if chart.get("aspects"):
        parts.append("Aspects: " + json.dumps(chart["aspects"]))
    if chart.get("elem_ratios"):
        parts.append("Element ratios: " + json.dumps(chart["elem_ratios"]))
    if chart.get("modality_ratios"):
        parts.append("Modality ratios: " + json.dumps(chart["modality_ratios"]))
    return "\n".join(parts) if parts else "No chart data"


def _prompt_for_reading(chart_text: str) -> str:
    return f"""You are an astrological interpreter. Given the following birth chart data, write a short astrological reading in plain language (2–4 sentences). Focus on temperament, strengths, and possible life themes. Be concise and neutral.

Chart data:
{chart_text}

Astrological reading:"""


def _call_ollama(prompt: str) -> str:
    url = f"{OLLAMA_URL}/api/generate"
    payload = {
        "model": LLM_MODEL,
        "prompt": prompt,
        "stream": False,
    }
    timeout = _ollama_timeout()
    r = requests.post(url, json=payload, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    return (data.get("response") or "").strip()


def _store_interpretation(cur, person_id: str, interpretation_text: str, prompt_hash: str | None = None) -> None:
    cur.execute(
        """
        INSERT INTO astro_interpretations (person_id, interpretation_text, model_name, prompt_hash, created_at)
        VALUES (%s, %s, %s, %s, NOW())
        ON CONFLICT (person_id) DO UPDATE
          SET interpretation_text = EXCLUDED.interpretation_text,
              model_name = EXCLUDED.model_name,
              prompt_hash = EXCLUDED.prompt_hash,
              created_at = NOW()
        """,
        (person_id, interpretation_text, LLM_MODEL, prompt_hash),
    )


def _consume_loop() -> None:
    dsn = _get_dsn()
    if not dsn:
        raise RuntimeError("PG_DSN or DATABASE_URL required")

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([KAFKA_TOPIC])

    print(f"Astro interpreter listening on topic '{KAFKA_TOPIC}' (group={KAFKA_GROUP_ID})...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue

            try:
                payload = json.loads(msg.value().decode("utf-8"))
                function = payload.get("function")
                if function != "astro.interpret":
                    consumer.commit(message=msg)
                    continue

                args = payload.get("args") or []
                person_id = (args[0] if args else payload.get("kwargs", {}).get("person_id"))
                if not person_id:
                    print("astro.interpret: missing person_id")
                    consumer.commit(message=msg)
                    continue

                person_id = str(person_id)

                conn = psycopg2.connect(dsn)
                conn.autocommit = False
                cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                try:
                    if _has_interpretation(cur, person_id):
                        consumer.commit(message=msg)
                        continue

                    chart = _load_chart(cur, person_id)
                    if not chart:
                        print(f"astro.interpret: no astro_features for person_id={person_id}")
                        conn.rollback()
                        cur.close()
                        conn.close()
                        consumer.commit(message=msg)
                        continue

                    chart_text = _format_chart_for_prompt(chart)
                    prompt = _prompt_for_reading(chart_text)
                    interpretation_text = _call_ollama(prompt)
                    if not interpretation_text:
                        print(f"astro.interpret: empty LLM response for person_id={person_id}")
                        conn.rollback()
                        cur.close()
                        conn.close()
                        consumer.commit(message=msg)
                        continue

                    _store_interpretation(cur, person_id, interpretation_text)
                    conn.commit()
                    print(f"✅ astro_interpretations: stored for person_id={person_id}")
                    consumer.commit(message=msg)
                finally:
                    cur.close()
                    conn.close()
            except Exception as exc:
                print(f"Astro interpret job failed: {exc}")
                import traceback
                traceback.print_exc()
                # Do not commit: message will be redelivered for retry
    finally:
        consumer.close()


if __name__ == "__main__":
    _consume_loop()
