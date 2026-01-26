package com.astroreason.ingest

import com.astroreason.core.Config
import com.astroreason.core.logProvenanceEvent
import com.astroreason.core.queue.JobStatus
import com.astroreason.core.queue.createJobQueue
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.util.concurrent.atomic.AtomicBoolean

fun main() {
    Config.initialize()
    
    val settings = Config.settings
    val groupId = System.getenv("KAFKA_GROUP_ID") ?: "worker-ingest"
    val jobQueue = createJobQueue(
        settings.kafkaBootstrapServers,
        "default",
        groupId = groupId,
        clientId = "worker-ingest"
    )
    
    val running = AtomicBoolean(true)
    Runtime.getRuntime().addShutdownHook(Thread {
        running.set(false)
        println("Shutdown signal received, stopping worker...")
    })

    println("Worker started, listening for jobs...")
    var idleBackoffMs = 100L
    
    while (running.get()) {
        val envelope = jobQueue.dequeue()
        if (envelope != null) {
            idleBackoffMs = 100L
            val job = envelope.job
            try {
                val startedAt = System.nanoTime()
                jobQueue.updateStatus(job.id, JobStatus.STARTED)
                
                when (job.function) {
                    "worker.ingest.parse_adb_xml" -> {
                        val objectUri = job.args.firstOrNull() 
                            ?: throw IllegalArgumentException("Missing object_uri")
                        val meta = job.kwargs
                        
                        parseAdbXml(objectUri, meta)
                        
                        jobQueue.updateStatus(job.id, JobStatus.FINISHED, result = "Success")
                        logProvenanceEvent(
                            stage = "ingest_job",
                            status = "ok",
                            durationMs = (System.nanoTime() - startedAt) / 1_000_000,
                            meta = mapOf("job_id" to job.id, "function" to job.function)
                        )
                    }
                    else -> {
                        throw IllegalArgumentException("Unknown function: ${job.function}")
                    }
                }
            } catch (e: Exception) {
                jobQueue.updateStatus(
                    job.id, 
                    JobStatus.FAILED, 
                    excInfo = e.message
                )
                logProvenanceEvent(
                    stage = "ingest_job",
                    status = "error",
                    durationMs = null,
                    error = e.message ?: "unknown_error",
                    meta = mapOf("job_id" to job.id, "function" to job.function)
                )
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
