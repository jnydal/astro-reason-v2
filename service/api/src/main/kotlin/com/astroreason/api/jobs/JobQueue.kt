package com.astroreason.api.jobs

import com.astroreason.core.queue.Job
import com.astroreason.core.queue.JobQueue
import com.astroreason.core.queue.createJobQueue
import com.astroreason.core.Config

class ApiJobQueue {
    private val jobQueue: JobQueue = createJobQueue(
        Config.settings.kafkaBootstrapServers,
        "default",
        groupId = null,
        clientId = "api-producer"
    )

    fun enqueueParseAdbXml(objectUri: String, sourceLabel: String = "upload"): Job {
        return jobQueue.enqueue(
            function = "worker.ingest.parse_adb_xml",
            objectUri,
            kwargs = mapOf("source" to sourceLabel),
            jobTimeout = 1800,
            failureTtl = 86400,
            resultTtl = 86400
        )
    }

    fun getJobStatus(jobId: String): Job? {
        return jobQueue.fetch(jobId)
    }
}
