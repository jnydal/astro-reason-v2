package com.astroreason.ingest

import com.astroreason.core.Config
import com.astroreason.core.logProvenanceEvent
import com.astroreason.core.queue.Job
import com.astroreason.core.queue.JobStatus
import com.astroreason.core.queue.createJobQueue
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

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
    
    println("Worker started, listening for jobs...")
    
    while (true) {
        val job = jobQueue.dequeue()
        if (job != null) {
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
            }
        }
    }
}
