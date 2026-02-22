package com.astroreason.api.stats

import com.astroreason.api.models.CorrelationJobResult
import com.astroreason.api.models.FeatureImportanceEntry
import com.astroreason.api.storage.createS3Storage
import com.astroreason.core.Config
import com.astroreason.core.queue.JobStatus
import com.astroreason.core.queue.createJobQueue
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.util.concurrent.atomic.AtomicBoolean

fun main() {
    Config.initialize()

    val settings = Config.settings
    val topic = System.getenv("KAFKA_STATS_TOPIC") ?: "stats"
    val groupId = System.getenv("KAFKA_GROUP_ID") ?: "stats-worker"
    val jobQueue = createJobQueue(
        settings.kafkaBootstrapServers,
        topic,
        groupId = groupId,
        clientId = "stats-worker"
    )
    val storage = createS3Storage()
    storage.ensureBucket()
    val json = Json { encodeDefaults = true }

    val running = AtomicBoolean(true)
    Runtime.getRuntime().addShutdownHook(Thread {
        running.set(false)
        println("Shutdown signal received, stopping stats worker...")
    })

    println("Stats worker started (with birth-year detrending), listening for jobs...")
    var idleBackoffMs = 100L

    while (running.get()) {
        val envelope = jobQueue.dequeue()
        if (envelope != null) {
            idleBackoffMs = 100L
            val job = envelope.job
            try {
                jobQueue.updateStatus(job.id, JobStatus.STARTED)
                when (job.function) {
                    "stats.correlation" -> {
                        val limit = job.kwargs["limit"]?.toIntOrNull()
                        val minSamples = job.kwargs["minSamples"]?.toIntOrNull() ?: 3
                        val mode = job.kwargs["mode"]?.takeIf { it in setOf("features", "interpretations") } ?: "features"
                        val embeddingsScope = job.kwargs["embeddingsScope"]?.takeIf { it in setOf("all", "qid_only", "wiki_only") } ?: "all"
                        val qidOnly = embeddingsScope == "qid_only"
                        val wikiOnly = embeddingsScope == "wiki_only"

                        val selection = when (mode) {
                            "interpretations" -> loadEmbeddingInterpretationRowsForCorrelation(limit, qidOnly = qidOnly, wikiOnly = wikiOnly)
                            else -> loadEmbeddingAstroRowsForCorrelation(limit, withBirthYear = true, qidOnly = qidOnly, wikiOnly = wikiOnly)
                        }
                        val allHaveBirthYear = selection.rows.isNotEmpty() && selection.rows.all { it.birthYear != null }
                        println("stats.correlation: mode=$mode, embeddingsScope=$embeddingsScope, rows=${selection.rows.size}, allHaveBirthYear=$allHaveBirthYear")

                        val correlation = buildEmbeddingCorrelationResponse(
                            rows = selection.rows,
                            embeddingDim = selection.embeddingDim,
                            minSamples = minSamples
                        )
                        val featureImportanceOriginal = buildFeatureImportanceFromCorrelation(correlation)

                        val (featureImportanceDetrendedList, s3Uri) = if (mode == "features" && selection.rows.isNotEmpty() && allHaveBirthYear) {
                            val detrended = detrendEmbeddingsByBirthYear(selection.rows)
                            val rowsDetrended = selection.rows.zip(detrended).map { (row, res) ->
                                ClusterRow(row.personId, res, row.astro, row.birthYear)
                            }
                            val correlationDetrended = buildEmbeddingCorrelationResponse(
                                rows = rowsDetrended,
                                embeddingDim = selection.embeddingDim,
                                minSamples = minSamples
                            )
                            val detrendedFi = buildFeatureImportanceFromCorrelation(correlationDetrended)
                            println("stats.correlation: featureImportanceDetrended computed (${detrendedFi.entries.size} entries)")
                            val uri = if (correlation.rows.isNotEmpty()) {
                                val correlationJson = json.encodeToString(correlation)
                                storage.putBytes(
                                    namespace = "stats-correlation",
                                    content = correlationJson.toByteArray(),
                                    contentType = "application/json",
                                    extension = "json"
                                )
                            } else null
                            detrendedFi.entries to uri
                        } else {
                            if (mode == "features" && selection.rows.isNotEmpty()) {
                                println("stats.correlation: featureImportanceDetrended skipped (allHaveBirthYear=$allHaveBirthYear)")
                            }
                            emptyList<FeatureImportanceEntry>() to if (correlation.rows.isNotEmpty()) {
                                val correlationJson = json.encodeToString(correlation)
                                storage.putBytes(
                                    namespace = "stats-correlation",
                                    content = correlationJson.toByteArray(),
                                    contentType = "application/json",
                                    extension = "json"
                                )
                            } else null
                        }

                        val result = CorrelationJobResult(
                            embeddingDim = correlation.embeddingDim,
                            astroFeatureOrder = correlation.astroFeatureOrder,
                            featureImportance = featureImportanceOriginal.entries,
                            featureImportanceDetrended = featureImportanceDetrendedList,
                            correlationResultVersion = 2,
                            s3Uri = s3Uri
                        )
                        jobQueue.updateStatus(job.id, JobStatus.FINISHED, result = json.encodeToString(result))
                    }
                    else -> {
                        throw IllegalArgumentException("Unknown function: ${job.function}")
                    }
                }
            } catch (e: Exception) {
                jobQueue.updateStatus(job.id, JobStatus.FAILED, excInfo = e.message)
                e.printStackTrace()
            } finally {
                jobQueue.ack(envelope)
            }
        } else {
            Thread.sleep(idleBackoffMs)
            idleBackoffMs = (idleBackoffMs * 2).coerceAtMost(2_000L)
        }
    }
}
