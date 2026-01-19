package com.astroreason.api.models

import kotlinx.serialization.Serializable

@Serializable
data class VersionInfo(
    val name: String = "astro-reason-api",
    val version: String = "0.1.0"
)

@Serializable
data class IngestResponse(
    val jobId: String,
    val objectUri: String
)

@Serializable
data class JobStatusResponse(
    val id: String,
    val status: String,
    val enqueuedAt: String? = null,
    val startedAt: String? = null,
    val endedAt: String? = null,
    val excInfo: String? = null,
    val result: String? = null
)

@Serializable
data class HealthResponse(
    val status: String = "ok"
)
