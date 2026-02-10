package com.astroreason.api.stats

import com.astroreason.api.models.CorrelationJobResult
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

    println("Stats worker started, listening for jobs...")
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

                        val selection = loadEmbeddingAstroRowsForCorrelation(limit)
                        val correlation = buildEmbeddingCorrelationResponse(
                            rows = selection.rows,
                            embeddingDim = selection.embeddingDim,
                            minSamples = minSamples
                        )
                        val featureImportance = buildFeatureImportanceFromCorrelation(correlation)

                        val s3Uri = if (correlation.rows.isNotEmpty()) {
                            val correlationJson = json.encodeToString(correlation)
                            storage.putBytes(
                                namespace = "stats-correlation",
                                content = correlationJson.toByteArray(),
                                contentType = "application/json",
                                extension = "json"
                            )
                        } else {
                            null
                        }

                        val result = CorrelationJobResult(
                            embeddingDim = correlation.embeddingDim,
                            astroFeatureOrder = correlation.astroFeatureOrder,
                            featureImportance = featureImportance.entries,
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
