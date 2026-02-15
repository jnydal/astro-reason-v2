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
    val nlpVectorOrder: List<String> = emptyList(),
    val astroFeatureOrder: List<String>,
    val rows: List<CorrelationFeatureRow>,
    val embeddingDim: Int = 0,
    val embeddingIndexOrder: List<Int> = emptyList()
)

@Serializable
data class CorrelationJobResult(
    val embeddingDim: Int,
    val astroFeatureOrder: List<String>,
    /** Original embeddings vs astro (backward compatible). */
    val featureImportance: List<FeatureImportanceEntry>,
    /** After detrending embeddings by birth year (empty if skipped or old worker). */
    val featureImportanceDetrended: List<FeatureImportanceEntry> = emptyList(),
    /** 2 = worker with detrending; missing or 1 = old worker. */
    val correlationResultVersion: Int = 1,
    val s3Uri: String? = null
)

@Serializable
data class CorrelationJobResponse(
    val jobId: String,
    val status: String,
    val enqueuedAt: String? = null,
    val startedAt: String? = null,
    val endedAt: String? = null,
    val excInfo: String? = null,
    val result: CorrelationJobResult? = null,
    val s3Url: String? = null
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
    val embedding: List<Double>,
    val astro: Map<String, Double>,
    val cluster: Int? = null
)

@Serializable
data class ExportResponse(
    val rows: List<ExportRow>,
    val embeddingIndexOrder: List<String>,
    val astroFeatureOrder: List<String>
)
