package com.astroreason.api.stats

import com.astroreason.api.models.*
import com.astroreason.core.DatabaseManager
import com.astroreason.core.schema.*
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.jsonPrimitive
import org.apache.commons.math3.distribution.TDistribution
import org.apache.commons.math3.ml.clustering.Clusterable
import org.apache.commons.math3.ml.clustering.KMeansPlusPlusClusterer
import org.apache.commons.math3.ml.distance.EuclideanDistance
import org.apache.commons.math3.random.JDKRandomGenerator
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation
import org.apache.commons.math3.stat.correlation.SpearmansCorrelation
import org.jetbrains.exposed.sql.SortOrder
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.select
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import java.util.Locale
import java.util.UUID
import kotlin.math.abs
import kotlin.math.sqrt

private val json = Json { ignoreUnknownKeys = true; isLenient = true }

val NLP_VECTOR_ORDER = listOf(
    "sound", "visual", "oral", "anal", "urethral", "skin", "muscular", "olfactory"
)

data class StatsRow(
    val personId: UUID,
    val nlp: Map<String, Double>,
    val astro: Map<String, Double>
)

data class ClusterRow(
    val personId: UUID,
    val embedding: List<Double>,
    val astro: Map<String, Double>
)

data class PersonPoint(
    val personId: UUID,
    val vector: DoubleArray
) : Clusterable {
    override fun getPoint(): DoubleArray = vector
}

fun loadNlpAstroRows(limit: Int? = null): List<StatsRow> {
    return transaction(DatabaseManager.getDatabase()) {
        val query = NlpVectors
            .join(AstroFeatures, org.jetbrains.exposed.sql.JoinType.INNER, NlpVectors.personId, AstroFeatures.id)
            .slice(NlpVectors.personId, NlpVectors.vectors, AstroFeatures.featureVec)
            .selectAll()

        if (limit != null) {
            query.limit(limit)
        }

        query.mapNotNull { row ->
            val nlpRaw = row[NlpVectors.vectors]
            val astroRaw = row[AstroFeatures.featureVec]
            val nlp = parseNlpVector(nlpRaw)
            val astro = parseDoubleMap(astroRaw)
            if (nlp.size != NLP_VECTOR_ORDER.size || astro.isEmpty()) {
                null
            } else {
                StatsRow(row[NlpVectors.personId], nlp, astro)
            }
        }
    }
}

fun loadEmbeddingAstroRows(limit: Int? = null, modelName: String? = null): List<ClusterRow> {
    return transaction(DatabaseManager.getDatabase()) {
        val query = Embeddings1024
            .join(AstroFeatures, org.jetbrains.exposed.sql.JoinType.INNER, Embeddings1024.personId, AstroFeatures.id)
            .slice(
                Embeddings1024.personId,
                Embeddings1024.modelName,
                Embeddings1024.vector,
                Embeddings1024.updatedAt,
                AstroFeatures.featureVec
            )
            .select {
                if (modelName != null) Embeddings1024.modelName eq modelName else Embeddings1024.modelName.isNotNull()
            }
            .orderBy(Embeddings1024.updatedAt, SortOrder.DESC_NULLS_LAST)

        if (limit != null) {
            query.limit(limit)
        }

        val rows = query.mapNotNull { row ->
            val embedding = parsePgVector(row[Embeddings1024.vector])
            val astro = parseDoubleMap(row[AstroFeatures.featureVec])
            if (embedding.isEmpty() || astro.isEmpty()) {
                null
            } else {
                ClusterRow(row[Embeddings1024.personId], embedding, astro)
            }
        }

        // If multiple model rows exist per person, keep the most recent
        rows.groupBy { it.personId }.values.mapNotNull { group ->
            group.firstOrNull()
        }
    }
}

fun buildCorrelationResponse(rows: List<StatsRow>, minSamples: Int = 3): CorrelationResponse {
    val astroFeatureOrder = rows.flatMap { it.astro.keys }.toSet().sorted()
    val pearson = PearsonsCorrelation()
    val spearman = SpearmansCorrelation()

    val responseRows = astroFeatureOrder.map { feature ->
        val stats = NLP_VECTOR_ORDER.associateWith { nlpKey ->
            val pairs = rows.mapNotNull { row ->
                val x = row.nlp[nlpKey]
                val y = row.astro[feature]
                if (x != null && y != null) x to y else null
            }

            if (pairs.size < minSamples) {
                CorrelationCell(n = pairs.size)
            } else {
                val xArr = pairs.map { it.first }.toDoubleArray()
                val yArr = pairs.map { it.second }.toDoubleArray()
                val pearsonR = safeCorrelation { pearson.correlation(xArr, yArr) }
                val spearmanR = safeCorrelation { spearman.correlation(xArr, yArr) }
                CorrelationCell(
                    n = pairs.size,
                    pearson = pearsonR,
                    pearsonP = pearsonR?.let { pValueForCorrelation(it, pairs.size) },
                    spearman = spearmanR,
                    spearmanP = spearmanR?.let { pValueForCorrelation(it, pairs.size) }
                )
            }
        }

        CorrelationFeatureRow(feature = feature, stats = stats)
    }

    return CorrelationResponse(
        nlpVectorOrder = NLP_VECTOR_ORDER,
        astroFeatureOrder = astroFeatureOrder,
        rows = responseRows
    )
}

fun buildFeatureImportance(rows: List<StatsRow>, minSamples: Int = 3): FeatureImportanceResponse {
    val correlation = buildCorrelationResponse(rows, minSamples)
    val entries = correlation.rows.map { row ->
        val values = row.stats.values.mapNotNull { it.pearson?.let { v -> abs(v) } }
        val meanAbs = if (values.isEmpty()) 0.0 else values.sum() / values.size
        FeatureImportanceEntry(feature = row.feature, meanAbsPearson = meanAbs, n = values.size)
    }.sortedByDescending { it.meanAbsPearson }

    return FeatureImportanceResponse(entries = entries)
}

fun buildClusterResponse(
    rows: List<ClusterRow>,
    k: Int,
    astroFeatureOrder: List<String>
): ClusterResponse {
    val rng = JDKRandomGenerator().apply { setSeed(42) }
    val clusterer = KMeansPlusPlusClusterer<PersonPoint>(
        k,
        100,
        EuclideanDistance(),
        rng
    )

    val points = rows.mapNotNull { row ->
        val astroVec = astroFeatureOrder.mapNotNull { row.astro[it] }
        if (astroVec.size != astroFeatureOrder.size) {
            null
        } else {
            val combined = (row.embedding + astroVec).toDoubleArray()
            PersonPoint(row.personId, combined)
        }
    }

    val clusters = clusterer.cluster(points)
    val assignments = mutableListOf<ClusterAssignment>()
    val centroids = clusters.mapIndexed { idx, cluster ->
        cluster.points.forEach { point ->
            assignments.add(ClusterAssignment(personId = point.personId.toString(), cluster = idx))
        }
        ClusterCentroid(cluster = idx, vector = cluster.center.point.toList())
    }

    return ClusterResponse(
        k = k,
        n = points.size,
        embeddingDim = rows.firstOrNull()?.embedding?.size ?: 0,
        astroFeatureOrder = astroFeatureOrder,
        assignments = assignments,
        centroids = centroids
    )
}

fun buildExportResponse(
    rows: List<StatsRow>,
    clusterAssignments: Map<UUID, Int>? = null
): ExportResponse {
    val astroFeatureOrder = rows.flatMap { it.astro.keys }.toSet().sorted()
    val exportRows = rows.map { row ->
        ExportRow(
            personId = row.personId.toString(),
            nlp = NLP_VECTOR_ORDER.associateWith { row.nlp[it] ?: 0.0 },
            astro = astroFeatureOrder.associateWith { row.astro[it] ?: 0.0 },
            cluster = clusterAssignments?.get(row.personId)
        )
    }

    return ExportResponse(
        rows = exportRows,
        nlpVectorOrder = NLP_VECTOR_ORDER,
        astroFeatureOrder = astroFeatureOrder
    )
}

fun buildExportCsv(
    export: ExportResponse,
    includeClusters: Boolean
): String {
    val header = buildList {
        add("person_id")
        addAll(export.nlpVectorOrder.map { "nlp_$it" })
        addAll(export.astroFeatureOrder.map { "astro_$it" })
        if (includeClusters) add("cluster_id")
    }

    val rows = export.rows.map { row ->
        val values = buildList {
            add(row.personId)
            addAll(export.nlpVectorOrder.map { fmt(row.nlp[it] ?: 0.0) })
            addAll(export.astroFeatureOrder.map { fmt(row.astro[it] ?: 0.0) })
            if (includeClusters) add(row.cluster?.toString() ?: "")
        }
        values.joinToString(",") { csvEscape(it) }
    }

    return (listOf(header.joinToString(",") { csvEscape(it) }) + rows).joinToString("\n")
}

private fun parseNlpVector(raw: String): Map<String, Double> {
    val parsed = parseDoubleMap(raw)
    return NLP_VECTOR_ORDER.mapNotNull { key ->
        parsed[key]?.let { key to it }
    }.toMap()
}

private fun parseDoubleMap(raw: String): Map<String, Double> {
    val element = runCatching { json.parseToJsonElement(raw) }.getOrNull() ?: return emptyMap()
    val obj = element as? JsonObject ?: return emptyMap()
    return obj.mapNotNull { (key, value) ->
        value.jsonPrimitive.doubleOrNull?.let { key to it }
    }.toMap()
}

internal fun parsePgVector(raw: String): List<Double> {
    val trimmed = raw.trim()
        .removePrefix("[")
        .removeSuffix("]")
        .removePrefix("(")
        .removeSuffix(")")
    if (trimmed.isBlank()) return emptyList()
    return trimmed.split(",").mapNotNull { entry ->
        entry.trim().takeIf { it.isNotEmpty() }?.toDoubleOrNull()
    }
}

private fun safeCorrelation(calc: () -> Double): Double? {
    val value = runCatching { calc() }.getOrNull()
    return value?.takeIf { !it.isNaN() && it.isFinite() }
}

private fun pValueForCorrelation(r: Double, n: Int): Double? {
    if (n < 3) return null
    val rClamped = when {
        r > 0.999999 -> 0.999999
        r < -0.999999 -> -0.999999
        else -> r
    }
    val t = rClamped * sqrt((n - 2).toDouble() / (1 - rClamped * rClamped))
    val dist = TDistribution((n - 2).toDouble())
    return 2.0 * (1.0 - dist.cumulativeProbability(abs(t)))
}

private fun fmt(value: Double): String =
    String.format(Locale.US, "%.6f", value)

private fun csvEscape(value: String): String {
    return if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
        "\"${value.replace("\"", "\"\"")}\""
    } else {
        value
    }
}
