package com.astroreason.ingest

import com.astroreason.core.Config
import com.astroreason.core.DatabaseManager
import com.astroreason.core.queue.Job
import com.astroreason.core.queue.JobStatus
import com.astroreason.core.queue.createJobQueue
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

fun main() {
    Config.initialize()
    
    val settings = Config.settings
    val jobQueue = createJobQueue(
        settings.redisUrl ?: "redis://redis:6379/0",
        "default"
    )
    
    println("Worker started, listening for jobs...")
    
    while (true) {
        val job = jobQueue.dequeue()
        if (job != null) {
            try {
                jobQueue.updateStatus(job.id, JobStatus.STARTED)
                
                when (job.function) {
                    "worker.ingest.parse_adb_xml" -> {
                        val objectUri = job.args.firstOrNull() 
                            ?: throw IllegalArgumentException("Missing object_uri")
                        val meta = job.kwargs
                        
                        parseAdbXml(objectUri, meta)
                        
                        jobQueue.updateStatus(job.id, JobStatus.FINISHED, result = "Success")
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
                e.printStackTrace()
            }
        }
    }
}
