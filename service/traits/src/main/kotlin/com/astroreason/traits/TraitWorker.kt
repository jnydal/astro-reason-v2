package com.astroreason.traits

import com.astroreason.core.Config
import com.astroreason.core.DatabaseManager
import com.astroreason.core.logProvenanceEvent
import com.astroreason.core.schema.*
import com.astroreason.core.queue.*
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.*
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean

fun main() {
    Config.initialize()
    
    val settings = Config.settings
    val groupId = System.getenv("KAFKA_GROUP_ID") ?: "traits-worker"
    val jobQueue = createJobQueue(
        settings.kafkaBootstrapServers,
        "traits",
        groupId = groupId,
        clientId = "traits-worker"
    )
    
    val scorer = TraitScorer(
        baseUrl = settings.ollamaUrl ?: "http://local-llm:11434",
        model = settings.llmModel
    )
    
    val running = AtomicBoolean(true)
    Runtime.getRuntime().addShutdownHook(Thread {
        running.set(false)
        println("Shutdown signal received, stopping trait worker...")
    })

    println("Trait worker started, listening for jobs...")
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
                    "traits.score_person" -> {
                        val personIdStr = job.args.firstOrNull()
                            ?: throw IllegalArgumentException("Missing person_id")
                        val personId = UUID.fromString(personIdStr)
                        
                        runBlocking {
                            scorePersonTraits(personId, scorer)
                        }
                        
                        jobQueue.updateStatus(job.id, JobStatus.FINISHED, result = "Success")
                        logProvenanceEvent(
                            personId = personId,
                            stage = "traits",
                            status = "ok",
                            count = 1,
                            durationMs = (System.nanoTime() - startedAt) / 1_000_000,
                            meta = mapOf("job_id" to job.id)
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
                    stage = "traits",
                    status = "error",
                    error = e.message ?: "unknown_error",
                    meta = mapOf("job_id" to job.id)
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

suspend fun scorePersonTraits(personId: UUID, scorer: TraitScorer) {
    // Fetch bio text
    val bioText = transaction(DatabaseManager.getDatabase()) {
        BioText.select {
            BioText.personId eq personId
        }.orderBy(BioText.updatedAt to SortOrder.DESC_NULLS_LAST)
            .firstOrNull()?.get(BioText.text)
            ?: throw IllegalStateException("No bio text found for person $personId")
    }

    // Score vectors (suspend, run outside DB transaction)
    val result = scorer.scoreVectorsBio(bioText)
    val promptHash = scorer.hashPrompt(bioText)

    // Store in nlp_vectors
    transaction(DatabaseManager.getDatabase()) {
        NlpVectors.insert {
            it[NlpVectors.personId] = personId
            it[NlpVectors.vectors] = Json.encodeToString(result.vectors)
            it[NlpVectors.dominant] = result.dominant.joinToString(",")
            it[NlpVectors.confidence] = result.confidence
            it[NlpVectors.modelName] = scorer.model
            it[NlpVectors.provider] = "ollama"
            it[NlpVectors.temperature] = 0.1
            it[NlpVectors.promptHash] = promptHash
        }
    }
}
