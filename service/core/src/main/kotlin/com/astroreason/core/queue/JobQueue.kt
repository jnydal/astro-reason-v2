package com.astroreason.core.queue

import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import java.util.*

@Serializable
data class Job(
    val id: String = UUID.randomUUID().toString(),
    val function: String,
    val args: List<String> = emptyList(),
    val kwargs: Map<String, String> = emptyMap(),
    val status: JobStatus = JobStatus.QUEUED,
    val enqueuedAt: Long = System.currentTimeMillis(),
    val startedAt: Long? = null,
    val endedAt: Long? = null,
    val result: String? = null,
    val excInfo: String? = null
)

enum class JobStatus {
    QUEUED,
    STARTED,
    FINISHED,
    FAILED
}

class JobQueue(private val jedisPool: JedisPool, private val queueName: String = "default") {
    
    fun enqueue(
        function: String,
        vararg args: String,
        kwargs: Map<String, String> = emptyMap(),
        jobTimeout: Int = 1800,
        failureTtl: Int = 86400,
        resultTtl: Int = 86400
    ): Job {
        val job = Job(
            function = function,
            args = args.toList(),
            kwargs = kwargs
        )
        
        jedisPool.resource.use { jedis ->
            val jobJson = Json.encodeToString(job)
            jedis.lpush("rq:queue:$queueName", jobJson)
            jedis.setex("rq:job:${job.id}", resultTtl, jobJson)
        }
        
        return job
    }
    
    fun fetch(jobId: String): Job? {
        return jedisPool.resource.use { jedis ->
            val jobJson = jedis.get("rq:job:$jobId")
            if (jobJson != null) {
                Json.decodeFromString<Job>(jobJson)
            } else {
                null
            }
        }
    }
    
    fun dequeue(): Job? {
        return jedisPool.resource.use { jedis ->
            val result = jedis.brpop(10, "rq:queue:$queueName")
            if (result != null && result.size >= 2) {
                val jobJson = result[1]
                Json.decodeFromString<Job>(jobJson)
            } else {
                null
            }
        }
    }
    
    fun updateStatus(jobId: String, status: JobStatus, result: String? = null, excInfo: String? = null) {
        val job = fetch(jobId) ?: return
        
        val updated = job.copy(
            status = status,
            startedAt = if (status == JobStatus.STARTED) System.currentTimeMillis() else job.startedAt,
            endedAt = if (status in listOf(JobStatus.FINISHED, JobStatus.FAILED)) System.currentTimeMillis() else job.endedAt,
            result = result,
            excInfo = excInfo
        )
        
        jedisPool.resource.use { jedis ->
            val jobJson = Json.encodeToString(updated)
            jedis.set("rq:job:$jobId", jobJson)
        }
    }
}

fun createJobQueue(redisUrl: String, queueName: String = "default"): JobQueue {
    val pool = JedisPool(redisUrl)
    return JobQueue(pool, queueName)
}
