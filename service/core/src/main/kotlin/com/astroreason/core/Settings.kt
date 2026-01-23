package com.astroreason.core

import kotlinx.serialization.Serializable

@Serializable
data class Settings(
    val appName: String = "astro-reason",
    val appEnv: String = System.getenv("APP_ENV") ?: "dev",
    val logLevel: String = System.getenv("LOG_LEVEL") ?: "INFO",
    
    // Postgres
    val pgDsn: String = System.getenv("PG_DSN") 
        ?: System.getenv("DATABASE_URL")
        ?: throw IllegalArgumentException("PG_DSN or DATABASE_URL must be set"),
    
    // Kafka
    val kafkaBootstrapServers: String = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "kafka:9092",
    
    // MinIO / S3
    val s3Endpoint: String? = System.getenv("MINIO_ENDPOINT") ?: System.getenv("S3_ENDPOINT"),
    val s3AccessKey: String? = System.getenv("MINIO_ACCESS_KEY") ?: System.getenv("S3_ACCESS_KEY"),
    val s3SecretKey: String? = System.getenv("MINIO_SECRET_KEY") ?: System.getenv("S3_SECRET_KEY"),
    val s3Bucket: String = System.getenv("MINIO_BUCKET_RAW") ?: System.getenv("S3_BUCKET") ?: "astro-reason",
    val s3UseSsl: Boolean = System.getenv("S3_USE_SSL")?.toBoolean() ?: false,
    
    // LLM / Embeddings
    val ollamaUrl: String? = System.getenv("OLLAMA_URL"),
    val llmModel: String = System.getenv("LLM_MODEL") ?: "qwen2.5:7b-instruct-q4_K_M",
    val embeddingsModel: String = System.getenv("EMBEDDINGS_MODEL") ?: "BAAI/bge-large-en-v1.5",
    
    // Wikipedia
    val wikiLangDefault: String = System.getenv("WIKI_LANG_DEFAULT") ?: "en",
    
    // Astro
    // Which backend to use for astro features: "swisseph" (default, high precision) or "fallback"
    val astroBackend: String = System.getenv("ASTRO_BACKEND") ?: "swisseph",
    // Optional path to Swiss Ephemeris data files (e.g. /opt/ephe), falls back to Docker ENV SE_EPHE_PATH
    val swephEphePath: String? = System.getenv("SWEPH_EPHE_PATH") ?: System.getenv("SE_EPHE_PATH")
)

fun loadSettings(): Settings = Settings()
