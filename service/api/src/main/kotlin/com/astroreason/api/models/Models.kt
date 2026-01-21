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

@Serializable
data class CorrelationCell(
    val n: Int,
    val pearson: Double? = null,
    val pearsonP: Double? = null,
    val spearman: Double? = null,
    val spearmanP: Double? = null
)

@Serializable
data class CorrelationFeatureRow(
    val feature: String,
    val stats: Map<String, CorrelationCell>
)

@Serializable
data class CorrelationResponse(
    val nlpVectorOrder: List<String>,
    val astroFeatureOrder: List<String>,
    val rows: List<CorrelationFeatureRow>
)

@Serializable
data class FeatureImportanceEntry(
    val feature: String,
    val meanAbsPearson: Double,
    val n: Int
)

@Serializable
data class FeatureImportanceResponse(
    val entries: List<FeatureImportanceEntry>
)

@Serializable
data class ClusterAssignment(
    val personId: String,
    val cluster: Int
)

@Serializable
data class ClusterCentroid(
    val cluster: Int,
    val vector: List<Double>
)

@Serializable
data class ClusterResponse(
    val k: Int,
    val n: Int,
    val embeddingDim: Int,
    val astroFeatureOrder: List<String>,
    val assignments: List<ClusterAssignment>,
    val centroids: List<ClusterCentroid>
)

@Serializable
data class ExportRow(
    val personId: String,
    val nlp: Map<String, Double>,
    val astro: Map<String, Double>,
    val cluster: Int? = null
)

@Serializable
data class ExportResponse(
    val rows: List<ExportRow>,
    val nlpVectorOrder: List<String>,
    val astroFeatureOrder: List<String>
)
