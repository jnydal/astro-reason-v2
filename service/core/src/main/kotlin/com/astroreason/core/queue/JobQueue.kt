package com.astroreason.core.queue

import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import com.astroreason.core.DatabaseManager
import com.astroreason.core.schema.JobStatusTable
import kotlinx.serialization.decodeFromString
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import java.time.Duration
import java.time.Instant
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

data class JobEnvelope(
    val job: Job,
    val topic: String,
    val partition: Int,
    val offset: Long
)

enum class JobStatus {
    QUEUED,
    STARTED,
    FINISHED,
    FAILED
}

class JobQueue(
    private val bootstrapServers: String,
    private val queueName: String = "default",
    private val groupId: String? = null,
    private val clientId: String = "astro-reason"
) {
    private val json = Json { encodeDefaults = true }
    private val kafkaTemplate: KafkaTemplate<String, String>
    private val consumer: KafkaConsumer<String, String>?

    init {
        val producerProps = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.CLIENT_ID_CONFIG to clientId
        )
        val producerFactory = DefaultKafkaProducerFactory<String, String>(producerProps)
        kafkaTemplate = KafkaTemplate(producerFactory)

        consumer = groupId?.let { gid ->
            val props = Properties()
            props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
            props[ConsumerConfig.GROUP_ID_CONFIG] = gid
            props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
            props[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false
            props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
            props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
            KafkaConsumer<String, String>(props).apply {
                subscribe(listOf(queueName))
            }
        }
    }

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

        persistJob(job)
        kafkaTemplate.send(queueName, job.id, json.encodeToString(job))

        return job
    }

    fun fetch(jobId: String): Job? {
        return transaction(DatabaseManager.getDatabase()) {
            JobStatusTable.select { JobStatusTable.id eq UUID.fromString(jobId) }
                .singleOrNull()
                ?.let { row ->
                    val args = runCatching {
                        json.decodeFromString<List<String>>(row[JobStatusTable.argsJson])
                    }.getOrElse { emptyList() }
                    val kwargs = runCatching {
                        json.decodeFromString<Map<String, String>>(row[JobStatusTable.kwargsJson])
                    }.getOrElse { emptyMap() }
                    Job(
                        id = row[JobStatusTable.id].value.toString(),
                        function = row[JobStatusTable.function],
                        args = args,
                        kwargs = kwargs,
                        status = JobStatus.valueOf(row[JobStatusTable.status]),
                        enqueuedAt = row[JobStatusTable.enqueuedAt],
                        startedAt = row[JobStatusTable.startedAt],
                        endedAt = row[JobStatusTable.endedAt],
                        result = row[JobStatusTable.result],
                        excInfo = row[JobStatusTable.excInfo]
                    )
                }
        }
    }

    fun dequeue(): JobEnvelope? {
        val kafkaConsumer = consumer ?: return null
        val records = kafkaConsumer.poll(Duration.ofSeconds(5))
        if (records.isEmpty) {
            return null
        }

        val record = records.first()
        val job = json.decodeFromString<Job>(record.value())
        return JobEnvelope(
            job = job,
            topic = record.topic(),
            partition = record.partition(),
            offset = record.offset()
        )
    }

    fun ack(envelope: JobEnvelope) {
        val kafkaConsumer = consumer ?: return
        val topicPartition = TopicPartition(envelope.topic, envelope.partition)
        kafkaConsumer.commitSync(
            mapOf(topicPartition to OffsetAndMetadata(envelope.offset + 1))
        )
    }

    fun updateStatus(jobId: String, status: JobStatus, result: String? = null, excInfo: String? = null) {
        val job = fetch(jobId) ?: return
        val now = System.currentTimeMillis()
        val startedAt = if (status == JobStatus.STARTED) now else job.startedAt
        val endedAt = if (status in listOf(JobStatus.FINISHED, JobStatus.FAILED)) now else job.endedAt

        transaction(DatabaseManager.getDatabase()) {
            JobStatusTable.update({ JobStatusTable.id eq UUID.fromString(jobId) }) {
                it[JobStatusTable.status] = status.name
                it[JobStatusTable.startedAt] = startedAt
                it[JobStatusTable.endedAt] = endedAt
                it[JobStatusTable.result] = result
                it[JobStatusTable.excInfo] = excInfo
                it[JobStatusTable.updatedAt] = Instant.now()
            }
        }
    }

    private fun persistJob(job: Job) {
        transaction(DatabaseManager.getDatabase()) {
            JobStatusTable.insert {
                it[id] = UUID.fromString(job.id)
                it[function] = job.function
                it[status] = job.status.name
                it[argsJson] = json.encodeToString(job.args)
                it[kwargsJson] = json.encodeToString(job.kwargs)
                it[enqueuedAt] = job.enqueuedAt
                it[startedAt] = job.startedAt
                it[endedAt] = job.endedAt
                it[result] = job.result
                it[excInfo] = job.excInfo
            }
        }
    }
}

fun createJobQueue(
    bootstrapServers: String,
    queueName: String = "default",
    groupId: String? = null,
    clientId: String = "astro-reason"
): JobQueue {
    return JobQueue(bootstrapServers, queueName, groupId, clientId)
}
