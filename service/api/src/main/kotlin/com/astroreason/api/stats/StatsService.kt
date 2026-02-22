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
import org.apache.commons.math3.stat.regression.SimpleRegression
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

private val EMBEDDING_DIM_PREFERENCE = listOf(1024, 768, 384, 1536)

data class ClusterRow(
    val personId: UUID,
    val embedding: List<Double>,
    val astro: Map<String, Double>,
    val birthYear: Int? = null
)

data class EmbeddingSelection(
    val embeddingDim: Int,
    val rows: List<ClusterRow>
)

data class PersonPoint(
    val personId: UUID,
    val vector: DoubleArray
) : Clusterable {
    override fun getPoint(): DoubleArray = vector
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

fun loadEmbeddingAstroRowsForCorrelation(
    limit: Int? = null,
    withBirthYear: Boolean = false,
    qidOnly: Boolean = false,
    wikiOnly: Boolean = false
): EmbeddingSelection {
    return transaction(DatabaseManager.getDatabase()) {
        val selectedDim = EMBEDDING_DIM_PREFERENCE.firstOrNull { dim ->
            embeddingTableHasRows(dim)
        } ?: return@transaction EmbeddingSelection(0, emptyList())

        val rows = loadEmbeddingAstroRowsForDim(
            selectedDim,
            limit,
            modelName = null,
            withBirthYear = withBirthYear,
            qidOnly = qidOnly,
            wikiOnly = wikiOnly
        )
        EmbeddingSelection(selectedDim, rows)
    }
}

/**
 * Loads (embedding, interpretation-derived features) for correlation.
 * Joins embeddings with astro_interpretations; each row gets synthetic features
 * "interpretation" = 1.0 and "interpretation:&lt;modelName&gt;" = 1.0.
 */
fun loadEmbeddingInterpretationRowsForCorrelation(
    limit: Int? = null,
    qidOnly: Boolean = false,
    wikiOnly: Boolean = false
): EmbeddingSelection {
    return transaction(DatabaseManager.getDatabase()) {
        val selectedDim = EMBEDDING_DIM_PREFERENCE.firstOrNull { dim ->
            embeddingTableHasRows(dim)
        } ?: return@transaction EmbeddingSelection(0, emptyList())

        val rows = loadEmbeddingInterpretationRowsForDim(selectedDim, limit, qidOnly = qidOnly, wikiOnly = wikiOnly)
        EmbeddingSelection(selectedDim, rows)
    }
}

fun buildEmbeddingCorrelationResponse(
    rows: List<ClusterRow>,
    embeddingDim: Int,
    minSamples: Int = 3
): CorrelationResponse {
    if (rows.isEmpty()) {
        return CorrelationResponse(
            nlpVectorOrder = emptyList(),
            astroFeatureOrder = emptyList(),
            rows = emptyList(),
            embeddingDim = 0,
            embeddingIndexOrder = emptyList()
        )
    }

    val effectiveDim = rows.first().embedding.size.takeIf { it > 0 } ?: embeddingDim
    val embeddingIndexOrder = (0 until effectiveDim).toList()
    val astroFeatureOrder = rows.flatMap { it.astro.keys }.toSet().sorted()
    val pearson = PearsonsCorrelation()
    val spearman = SpearmansCorrelation()

    val responseRows = astroFeatureOrder.map { feature ->
        val stats = embeddingIndexOrder.associate { idx ->
            val pairs = rows.mapNotNull { row ->
                val x = row.embedding.getOrNull(idx)
                val y = row.astro[feature]
                if (x != null && y != null) x to y else null
            }

            val cell = if (pairs.size < minSamples) {
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

            idx.toString() to cell
        }

        CorrelationFeatureRow(feature = feature, stats = stats)
    }

    return CorrelationResponse(
        nlpVectorOrder = emptyList(),
        astroFeatureOrder = astroFeatureOrder,
        rows = responseRows,
        embeddingDim = effectiveDim,
        embeddingIndexOrder = embeddingIndexOrder
    )
}

fun buildFeatureImportanceFromCorrelation(correlation: CorrelationResponse): FeatureImportanceResponse {
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
    rows: List<ClusterRow>,
    clusterAssignments: Map<UUID, Int>? = null
): ExportResponse {
    if (rows.isEmpty()) {
        return ExportResponse(
            rows = emptyList(),
            embeddingIndexOrder = emptyList(),
            astroFeatureOrder = emptyList()
        )
    }
    val astroFeatureOrder = rows.flatMap { it.astro.keys }.toSet().sorted()
    val dim = rows.maxOfOrNull { it.embedding.size } ?: 0
    val embeddingIndexOrder = (0 until dim).map { it.toString() }
    val exportRows = rows.map { row ->
        ExportRow(
            personId = row.personId.toString(),
            embedding = row.embedding,
            astro = astroFeatureOrder.associateWith { row.astro[it] ?: 0.0 },
            cluster = clusterAssignments?.get(row.personId)
        )
    }

    return ExportResponse(
        rows = exportRows,
        embeddingIndexOrder = embeddingIndexOrder,
        astroFeatureOrder = astroFeatureOrder
    )
}

fun buildExportCsv(
    export: ExportResponse,
    includeClusters: Boolean
): String {
    val header = buildList {
        add("person_id")
        addAll(export.embeddingIndexOrder.map { "embedding_$it" })
        addAll(export.astroFeatureOrder.map { "astro_$it" })
        if (includeClusters) add("cluster_id")
    }

    val rows = export.rows.map { row ->
        val values = buildList {
            add(row.personId)
            addAll(export.embeddingIndexOrder.mapIndexed { idx, _ ->
                fmt(row.embedding.getOrNull(idx) ?: 0.0)
            })
            addAll(export.astroFeatureOrder.map { fmt(row.astro[it] ?: 0.0) })
            if (includeClusters) add(row.cluster?.toString() ?: "")
        }
        values.joinToString(",") { csvEscape(it) }
    }

    return (listOf(header.joinToString(",") { csvEscape(it) }) + rows).joinToString("\n")
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

private fun embeddingTableHasRows(dim: Int): Boolean {
    val query = when (dim) {
        1024 -> Embeddings1024.slice(Embeddings1024.personId).selectAll().limit(1)
        768 -> Embeddings768.slice(Embeddings768.personId).selectAll().limit(1)
        384 -> Embeddings384.slice(Embeddings384.personId).selectAll().limit(1)
        1536 -> Embeddings1536.slice(Embeddings1536.personId).selectAll().limit(1)
        else -> return false
    }
    return query.firstOrNull() != null
}

/**
 * Detrends embeddings by birth year: for each dimension, fits
 * embedding_dim = β₀ + β₁ × birth_year and returns residuals.
 * Rows must all have non-null birthYear; otherwise returns original embeddings unchanged.
 */
fun detrendEmbeddingsByBirthYear(rows: List<ClusterRow>): List<List<Double>> {
    if (rows.isEmpty() || rows.any { it.birthYear == null }) return rows.map { it.embedding }
    val dims = rows.first().embedding.size
    if (dims == 0) return rows.map { it.embedding }
    val regression = SimpleRegression()
    return (0 until dims).map { d ->
        regression.clear()
        rows.forEach { r ->
            regression.addData(r.birthYear!!.toDouble(), r.embedding.getOrNull(d) ?: 0.0)
        }
        rows.map { row ->
            val pred = regression.slope * row.birthYear!! + regression.intercept
            (row.embedding.getOrNull(d) ?: 0.0) - pred
        }
    }.let { byDim ->
        (0 until rows.size).map { i -> byDim.map { it[i] } }
    }
}

private fun loadEmbeddingAstroRowsForDim(
    dim: Int,
    limit: Int? = null,
    modelName: String? = null,
    withBirthYear: Boolean = false,
    qidOnly: Boolean = false,
    wikiOnly: Boolean = false
): List<ClusterRow> {
    val (personCol, modelCol, vectorCol, updatedCol, sourceCol, tableName) = when (dim) {
        1024 -> EmbeddingColumns(
            Embeddings1024.personId, Embeddings1024.modelName, Embeddings1024.vector, Embeddings1024.updatedAt, Embeddings1024.sourceCol, Embeddings1024.tableName
        )
        768 -> EmbeddingColumns(
            Embeddings768.personId, Embeddings768.modelName, Embeddings768.vector, Embeddings768.updatedAt, Embeddings768.sourceCol, Embeddings768.tableName
        )
        384 -> EmbeddingColumns(
            Embeddings384.personId, Embeddings384.modelName, Embeddings384.vector, Embeddings384.updatedAt, Embeddings384.sourceCol, Embeddings384.tableName
        )
        1536 -> EmbeddingColumns(
            Embeddings1536.personId, Embeddings1536.modelName, Embeddings1536.vector, Embeddings1536.updatedAt, Embeddings1536.sourceCol, Embeddings1536.tableName
        )
        else -> return emptyList()
    }

    val embeddingTable = when (tableName) {
        Embeddings1024.tableName -> Embeddings1024
        Embeddings768.tableName -> Embeddings768
        Embeddings384.tableName -> Embeddings384
        Embeddings1536.tableName -> Embeddings1536
        else -> return emptyList()
    }
    var baseJoin = embeddingTable.join(AstroFeatures, org.jetbrains.exposed.sql.JoinType.INNER, personCol, AstroFeatures.id)
    if (qidOnly) {
        baseJoin = baseJoin.join(EntityLink, org.jetbrains.exposed.sql.JoinType.INNER, personCol, EntityLink.id)
    }

    val query = if (withBirthYear) {
        baseJoin
            .join(Birth, org.jetbrains.exposed.sql.JoinType.INNER, AstroFeatures.id, Birth.id)
            .slice(personCol, modelCol, vectorCol, updatedCol, AstroFeatures.featureVec, Birth.date)
            .select {
                val base = if (modelName != null) modelCol eq modelName else modelCol.isNotNull()
                if (wikiOnly) base and (sourceCol like "%fetch_bio%") else base
            }
            .orderBy(updatedCol, SortOrder.DESC_NULLS_LAST)
    } else {
        baseJoin
            .slice(personCol, modelCol, vectorCol, updatedCol, AstroFeatures.featureVec)
            .select {
                val base = if (modelName != null) modelCol eq modelName else modelCol.isNotNull()
                if (wikiOnly) base and (sourceCol like "%fetch_bio%") else base
            }
            .orderBy(updatedCol, SortOrder.DESC_NULLS_LAST)
    }

    if (limit != null) {
        query.limit(limit)
    }

    val rows = if (withBirthYear) {
        query.mapNotNull { row ->
            val embedding = parsePgVector(row[vectorCol])
            val astro = parseDoubleMap(row[AstroFeatures.featureVec])
            val birthDate = row[Birth.date]
            if (embedding.isEmpty() || astro.isEmpty() || birthDate == null) {
                null
            } else {
                ClusterRow(row[personCol], embedding, astro, birthYear = birthDate.year)
            }
        }
    } else {
        query.mapNotNull { row ->
            val embedding = parsePgVector(row[vectorCol])
            val astro = parseDoubleMap(row[AstroFeatures.featureVec])
            if (embedding.isEmpty() || astro.isEmpty()) {
                null
            } else {
                ClusterRow(row[personCol], embedding, astro)
            }
        }
    }

    return rows.groupBy { it.personId }.values.mapNotNull { group ->
        group.firstOrNull()
    }
}

private fun loadEmbeddingInterpretationRowsForDim(
    dim: Int,
    limit: Int? = null,
    qidOnly: Boolean = false,
    wikiOnly: Boolean = false
): List<ClusterRow> {
    val (personCol, modelCol, vectorCol, updatedCol, sourceCol, tableName) = when (dim) {
        1024 -> EmbeddingColumns(
            Embeddings1024.personId, Embeddings1024.modelName, Embeddings1024.vector, Embeddings1024.updatedAt, Embeddings1024.sourceCol, Embeddings1024.tableName
        )
        768 -> EmbeddingColumns(
            Embeddings768.personId, Embeddings768.modelName, Embeddings768.vector, Embeddings768.updatedAt, Embeddings768.sourceCol, Embeddings768.tableName
        )
        384 -> EmbeddingColumns(
            Embeddings384.personId, Embeddings384.modelName, Embeddings384.vector, Embeddings384.updatedAt, Embeddings384.sourceCol, Embeddings384.tableName
        )
        1536 -> EmbeddingColumns(
            Embeddings1536.personId, Embeddings1536.modelName, Embeddings1536.vector, Embeddings1536.updatedAt, Embeddings1536.sourceCol, Embeddings1536.tableName
        )
        else -> return emptyList()
    }

    val embeddingTable = when (tableName) {
        Embeddings1024.tableName -> Embeddings1024
        Embeddings768.tableName -> Embeddings768
        Embeddings384.tableName -> Embeddings384
        Embeddings1536.tableName -> Embeddings1536
        else -> return emptyList()
    }
    var baseJoin = embeddingTable.join(AstroInterpretations, org.jetbrains.exposed.sql.JoinType.INNER, personCol, AstroInterpretations.id)
    if (qidOnly) {
        baseJoin = baseJoin.join(EntityLink, org.jetbrains.exposed.sql.JoinType.INNER, personCol, EntityLink.id)
    }
    val query = baseJoin
        .slice(personCol, modelCol, vectorCol, updatedCol, AstroInterpretations.modelName)
        .select {
            if (wikiOnly) (modelCol.isNotNull()) and (sourceCol like "%fetch_bio%") else modelCol.isNotNull()
        }
        .orderBy(updatedCol, SortOrder.DESC_NULLS_LAST)

    if (limit != null) {
        query.limit(limit)
    }

    val rows = query.mapNotNull { row ->
        val embedding = parsePgVector(row[vectorCol])
        val modelName = row[AstroInterpretations.modelName]
        if (embedding.isEmpty()) {
            null
        } else {
            val astro = buildMap {
                put("interpretation", 1.0)
                put("interpretation:$modelName", 1.0)
            }
            ClusterRow(row[personCol], embedding, astro)
        }
    }

    return rows.groupBy { it.personId }.values.mapNotNull { group ->
        group.firstOrNull()
    }
}

private data class EmbeddingColumns(
    val personId: org.jetbrains.exposed.sql.Column<UUID>,
    val modelName: org.jetbrains.exposed.sql.Column<String>,
    val vector: org.jetbrains.exposed.sql.Column<String>,
    val updatedAt: org.jetbrains.exposed.sql.Column<*>,
    val sourceCol: org.jetbrains.exposed.sql.Column<String?>,
    val tableName: String
)

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
