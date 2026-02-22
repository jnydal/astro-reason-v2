package com.astroreason.resolver

import com.astroreason.core.Config
import com.astroreason.core.DatabaseManager
import com.astroreason.core.schema.*
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.statement.bodyAsText
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.HttpTimeout
import io.ktor.client.plugins.expectSuccess
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.plugins.defaultRequest
import io.ktor.client.plugins.timeout
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import kotlinx.coroutines.delay
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import java.time.Instant
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import java.io.File
import java.util.concurrent.TimeUnit
import java.time.format.DateTimeFormatter
import java.util.*
import java.util.Properties
import kotlin.random.Random
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.common.TopicPartition

@Serializable
data class WikidataSearchResult(
    val search: List<WikidataItem> = emptyList()
)

@Serializable
data class WikidataItem(
    val id: String,
    val label: String? = null
)

@Serializable
data class WikidataEntityData(
    val entities: Map<String, WikidataEntity> = emptyMap()
)

@Serializable
data class WikidataEntity(
    val sitelinks: Map<String, WikidataSitelink> = emptyMap(),
    val claims: Map<String, List<WikidataClaim>> = emptyMap()
)

@Serializable
data class WikidataSitelink(
    val title: String? = null
)

@Serializable
data class WikidataClaim(
    val mainsnak: WikidataMainSnak? = null
)

@Serializable
data class WikidataMainSnak(
    val datavalue: WikidataDataValue? = null
)

@Serializable
data class WikidataDataValue(
    val value: JsonObject = JsonObject(emptyMap())
)

class QidResolver {
    private val minIntervalMs = ((System.getenv("WIKIDATA_MIN_INTERVAL_SEC") ?: "1.0").toDouble() * 1000).toLong()
    private val jitterMs = ((System.getenv("WIKIDATA_JITTER_SEC") ?: "0.2").toDouble() * 1000).toLong()
    private var nextRequestTimeMs = 0L
    private val userAgent = System.getenv("WIKI_USER_AGENT") ?: "astro-reason/0.1 (contact: jnydal@gmail.com)"

    private val client = HttpClient(CIO) {
        install(ContentNegotiation) {
            json(Json {
                ignoreUnknownKeys = true
                isLenient = true
            })
        }
        install(HttpTimeout)
        defaultRequest {
            header(HttpHeaders.UserAgent, userAgent)
        }
    }

    private suspend fun waitForRateLimit() {
        val now = System.currentTimeMillis()
        if (now < nextRequestTimeMs) {
            delay(nextRequestTimeMs - now)
        }
        if (jitterMs > 0) {
            delay(Random.nextLong(0, jitterMs + 1))
        }
        nextRequestTimeMs = System.currentTimeMillis() + minIntervalMs
    }

    private fun extractWikidataDate(timeValue: String): String? {
        val match = Regex("""[+-]\d{4}-\d{2}-\d{2}""").find(timeValue)
        return match?.value?.removePrefix("+")
    }

    private fun normalizeName(name: String): String {
        val cleaned = name.trim()
        val commaIndex = cleaned.indexOf(',')
        if (commaIndex < 0) return cleaned
        val last = cleaned.substring(0, commaIndex).trim()
        val first = cleaned.substring(commaIndex + 1).trim()
        if (first.isBlank()) return cleaned
        return "$first $last"
    }

    /** Common name prefixes/titles that often cause Wikidata search to return no results. */
    private val titlePatterns = listOf(
        "conte ", "count ", "countess ", "dr. ", "dr ", "professor ", "prof. ", "prof ",
        "sir ", "dame ", "lord ", "lady ", "duke ", "duchess ", "prince ", "princess ",
        "baron ", "baroness ", "earl ", "marquis ", "marchioness ", "don ", "doña "
    ).map { it to Regex("\\b${Regex.escape(it)}", RegexOption.IGNORE_CASE) }

    /**
     * Strips common titles from a name to improve Wikidata search match rate.
     * E.g. "Conte Luigi Cadorna" -> "Luigi Cadorna".
     */
    private fun stripTitles(name: String): String {
        var result = name.trim()
        for ((_, pattern) in titlePatterns) {
            result = pattern.replace(result, " ").replace(Regex("\\s+"), " ").trim()
        }
        return result
    }

    /**
     * Returns search query variants to try (in order). Wikidata search often fails when
     * titles like "Conte" are included; stripping them yields better results.
     */
    private fun searchNameVariants(fullName: String): List<String> {
        val normalized = normalizeName(fullName)
        val stripped = stripTitles(normalized)
        val variants = mutableListOf<String>()
        if (normalized.isNotBlank()) variants.add(normalized)
        if (fullName.isNotBlank()) variants.add(fullName)
        if (stripped.isNotBlank() && stripped !in variants) variants.add(stripped)
        return variants.distinct()
    }

    /**
     * Returns false for event names, accidents, disasters, roles, and other entries that
     * won't resolve to a person with matching birth data (P569) and sensible Wikipedia bio.
     * Goal: process all people with birth data and bio; skip only when resolution won't yield useful data.
     */
    private fun looksLikePerson(name: String): Boolean {
        val n = name.trim()
        if (n.isEmpty()) return false
        if (n[0].isDigit()) return false
        val lower = n.lowercase()
        val skipPatterns = listOf(
            "accident:", "derailment", "academic:", "earthquake", "attacks survivor",
            "explosion", "missile strike", "disaster", "victim", "vocation :", "role :",
            "nature: ", "nature:", "helicopter crash", "train derailment", "train crash",
            "bus crash", "plane crash", "gas explosion", "shopping center strike"
        )
        return !skipPatterns.any { lower.contains(it) }
    }
    
    suspend fun searchQid(name: String): List<WikidataItem> {
        waitForRateLimit()
        val response = client.get("https://www.wikidata.org/w/api.php") {
            parameter("action", "wbsearchentities")
            parameter("language", "en")
            parameter("format", "json")
            parameter("type", "item")
            parameter("search", name)
            timeout {
                requestTimeoutMillis = 20000
            }
        }.body<WikidataSearchResult>()
        
        return response.search
    }
    
    /**
     * Checks if Wikidata entity P569 (birth date) matches our dobIso (yyyy-MM-dd).
     * When verbose=true, logs each step to help diagnose Wikidata response variations.
     */
    private suspend fun dobMatches(qid: String, dobIso: String?, verbose: Boolean = false): Boolean {
        if (dobIso.isNullOrBlank()) {
            if (verbose) println("  [dobMatches] qid=$qid: dobIso is null/blank, skip")
            return false
        }

        return try {
            waitForRateLimit()
            val bodyText = client.get("https://www.wikidata.org/wiki/Special:EntityData/$qid.json") {
                timeout {
                    requestTimeoutMillis = 20000
                }
            }.bodyAsText()

            if (verbose) println("  [dobMatches] qid=$qid: fetched entity JSON (${bodyText.length} chars)")

            val root = Json { ignoreUnknownKeys = true; isLenient = true }.parseToJsonElement(bodyText).jsonObject
            val entities = root["entities"]?.jsonObject
            if (entities == null) {
                if (verbose) println("  [dobMatches] qid=$qid: entities block missing")
                return false
            }
            val entity = entities[qid]?.jsonObject
            if (entity == null) {
                if (verbose) println("  [dobMatches] qid=$qid: entity not in response (keys: ${entities.keys})")
                return false
            }
            val claims = entity["claims"]?.jsonObject
            if (claims == null) {
                if (verbose) println("  [dobMatches] qid=$qid: claims block missing")
                return false
            }
            val p569Array = claims["P569"]?.jsonArray
            if (p569Array == null || p569Array.isEmpty()) {
                if (verbose) println("  [dobMatches] qid=$qid: P569 (birth date) missing (claim keys: ${claims.keys})")
                return false
            }
            val firstClaim = p569Array.firstOrNull()?.jsonObject
            if (firstClaim == null) {
                if (verbose) println("  [dobMatches] qid=$qid: first P569 claim not an object")
                return false
            }
            val datavalue = firstClaim["mainsnak"]?.jsonObject?.get("datavalue")?.jsonObject
            if (datavalue == null) {
                if (verbose) println("  [dobMatches] qid=$qid: P569 mainsnak.datavalue missing")
                return false
            }
            val value = datavalue["value"]
            if (value == null) {
                if (verbose) println("  [dobMatches] qid=$qid: P569 datavalue.value missing")
                return false
            }
            // value can be JsonObject (time has {"time":"+1850-09-04T00:00:00Z"}) or primitive
            val timeValue: String? = when (val v = value) {
                is kotlinx.serialization.json.JsonObject -> v["time"]?.jsonPrimitive?.content
                is kotlinx.serialization.json.JsonPrimitive -> v.content
                else -> v.toString().trim('"')
            }
            if (timeValue.isNullOrBlank()) {
                if (verbose) println("  [dobMatches] qid=$qid: P569 value has no time (value type: ${value::class.simpleName}, keys: ${(value as? kotlinx.serialization.json.JsonObject)?.keys})")
                return false
            }
            val extracted = extractWikidataDate(timeValue)
            if (extracted == null) {
                if (verbose) println("  [dobMatches] qid=$qid: could not extract date from timeValue='$timeValue' (regex expects [+-]yyyy-mm-dd)")
                return false
            }
            val matches = dobIso == extracted
            if (verbose) println("  [dobMatches] qid=$qid: timeValue='$timeValue' -> extracted='$extracted' vs dobIso='$dobIso' -> match=$matches")
            matches
        } catch (e: Exception) {
            if (verbose || System.getenv("RESOLVER_DEBUG") == "1") {
                println("  [dobMatches] qid=$qid dob=$dobIso EXCEPTION: ${e.message}")
                e.printStackTrace()
            }
            false
        }
    }

    /**
     * Records a person whose first QID resolution attempt failed. Resolver excludes these
     * from future retries; a future LLM post-processing job will handle them.
     */
    private fun markFailedQidLookup(personId: java.util.UUID, reason: String, detailsJson: String?) {
        transaction(DatabaseManager.getDatabase()) {
            FailedQidLookup.insertIgnore {
                it[FailedQidLookup.id] = personId
                it[FailedQidLookup.failureReason] = reason
                it[FailedQidLookup.detailsJson] = detailsJson
            }
        }
    }

    
    suspend fun resolveQids(limit: Int = 500) {
        data class PendingPerson(
            val personId: UUID,
            val fullName: String,
            val dobIso: String?
        )

        val pending = transaction(DatabaseManager.getDatabase()) {
            PersonRaw
                .innerJoin(Birth, { PersonRaw.id }, { Birth.id })
                .leftJoin(EntityLink, { PersonRaw.id }, { EntityLink.id })
                .leftJoin(FailedQidLookup, { PersonRaw.id }, { FailedQidLookup.id })
                .slice(PersonRaw.id, PersonRaw.name, Birth.date)
                .select { EntityLink.id.isNull() and FailedQidLookup.id.isNull() }
                .orderBy(PersonRaw.createdAt to SortOrder.ASC, PersonRaw.name to SortOrder.ASC)
                .limit(limit * 10)
                .map { row ->
                    PendingPerson(
                        personId = row[PersonRaw.id].value,
                        fullName = row[PersonRaw.name],
                        dobIso = row[Birth.date]?.format(DateTimeFormatter.ISO_DATE)
                    )
                }
                .filter { person -> looksLikePerson(person.fullName) }
                .take(limit)
        }

        data class ResolvedQid(
            val personId: UUID,
            val qid: String,
            val candidates: List<WikidataItem>
        )

        val resolved = mutableListOf<ResolvedQid>()

        for (person in pending) {
            try {
                var candidates: List<WikidataItem> = emptyList()
                for (query in searchNameVariants(person.fullName)) {
                    candidates = searchQid(query)
                    if (candidates.isNotEmpty()) break
                }

                if (candidates.isEmpty()) {
                    markFailedQidLookup(person.personId, "no_candidates", null)
                    continue
                }

                var qid: String? = null
                // Only accept when Wikidata entity has matching birth date (P569).
                // No fallback to first candidate: prevents linking events/non-persons.
                for (candidate in candidates.take(10)) {
                    if (dobMatches(candidate.id, person.dobIso)) {
                        qid = candidate.id
                        break
                    }
                }

                if (qid != null) {
                    resolved.add(
                        ResolvedQid(
                            personId = person.personId,
                            qid = qid,
                            candidates = candidates.take(10)
                        )
                    )
                } else {
                    val details = Json.encodeToString(mapOf("candidates" to candidates.take(10)))
                    markFailedQidLookup(person.personId, "no_dob_match", details)
                }
            } catch (e: Exception) {
                val details = Json.encodeToString(mapOf("error" to (e.message ?: "unknown")))
                markFailedQidLookup(person.personId, "api_error", details)
                if (System.getenv("RESOLVER_DEBUG") == "1") {
                    println("  [resolveQids] person=${person.personId} EXCEPTION: ${e.message}")
                    e.printStackTrace()
                }
            }
        }

        if (resolved.isNotEmpty()) {
            transaction(DatabaseManager.getDatabase()) {
                for (item in resolved) {
                    val candidatesJson = Json.encodeToString(item.candidates)
                    BioText.insertIgnore {
                        it[BioText.personId] = item.personId
                        it[BioText.revId] = 0L
                        it[BioText.qid] = item.qid
                    }
                    BioText.update({ (BioText.personId eq item.personId) and (BioText.revId eq 0L) }) {
                        it[BioText.qid] = item.qid
                    }
                    EntityLink.insertIgnore {
                        it[EntityLink.id] = item.personId
                        it[EntityLink.qid] = item.qid
                        it[EntityLink.method] = "resolver"
                        it[EntityLink.score] = null
                        it[EntityLink.candidatesJson] = candidatesJson
                        it[EntityLink.decidedAt] = Instant.now()
                    }
                    EntityLink.update({ EntityLink.id eq item.personId }) {
                        it[EntityLink.qid] = item.qid
                        it[EntityLink.method] = "resolver"
                        it[EntityLink.score] = null
                        it[EntityLink.candidatesJson] = candidatesJson
                        it[EntityLink.decidedAt] = Instant.now()
                    }
                }
            }
        }

        if (resolved.isEmpty() && pending.isNotEmpty()) {
            println("⚠️ Resolved 0 QIDs (${pending.size} pending). Set RESOLVER_DEBUG=1 and check logs for dobMatches failures.")
            if (System.getenv("RESOLVER_DEBUG") == "1") {
                val first = pending.first()
                runDiagnosticFirstPerson(first.fullName, first.dobIso)
            }
        } else {
            println("✅ Resolved ${resolved.size} QIDs")
        }
    }

    /**
     * Runs a verbose diagnostic for the first pending person to capture Wikidata response
     * variations and pinpoint why dobMatches fails. Only runs when RESOLVER_DEBUG=1.
     */
    private suspend fun runDiagnosticFirstPerson(fullName: String, dobIso: String?) {
        val variants = searchNameVariants(fullName)
        println("")
        println("=== RESOLVER DIAGNOSTIC (first pending person) ===")
        println("  name: '$fullName'")
        println("  dobIso: '$dobIso'")
        println("  search variants: $variants")
        var candidates: List<WikidataItem> = emptyList()
        for (query in variants) {
            candidates = searchQid(query)
            println("  search('$query') -> ${candidates.size} candidates")
            if (candidates.isNotEmpty()) break
        }
        println("  candidate QIDs: ${candidates.take(5).map { it.id }.joinToString(", ")}")
        for ((i, c) in candidates.take(5).withIndex()) {
            println("  --- candidate ${i + 1}: ${c.id} (${c.label ?: "no label"}) ---")
            dobMatches(c.id, dobIso, verbose = true)
        }
        println("=== END DIAGNOSTIC ===")
        println("")
    }
    
    /**
     * Returns true if any ingest job (worker.ingest.parse_adb_xml) is QUEUED or STARTED.
     * Used to gate fetch-bio: skip producing to embeddings topic while ingest is active,
     * to avoid parallel producer load on Kafka.
     */
    fun hasActiveIngestJobs(): Boolean {
        return transaction(DatabaseManager.getDatabase()) {
            JobStatusTable.select {
                (JobStatusTable.function eq "worker.ingest.parse_adb_xml") and
                (JobStatusTable.status inList listOf("QUEUED", "STARTED"))
            }.limit(1).firstOrNull() != null
        }
    }

    /**
     * Returns true if embeddings-worker consumer lag on embeddings topic exceeds threshold.
     * Used to gate fetch-bio: wait for embeddings worker to drain backlog before adding more.
     * If EMBEDDINGS_LAG_THRESHOLD is 0 or unset, always returns false (no lag check).
     * On Kafka/Admin errors, returns false to avoid blocking fetch-bio.
     */
    fun hasHighEmbeddingsLag(): Boolean {
        val threshold = System.getenv("EMBEDDINGS_LAG_THRESHOLD")?.toLongOrNull() ?: 0L
        if (threshold <= 0) return false

        val bootstrap = Config.settings.kafkaBootstrapServers
        val topic = System.getenv("KAFKA_EMBEDDINGS_TOPIC") ?: "embeddings"
        val groupId = System.getenv("KAFKA_EMBEDDINGS_GROUP_ID") ?: "embeddings-worker"

        return try {
            val props = Properties().apply {
                put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
                put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000)
            }
            AdminClient.create(props).use { admin ->
                val partitions = admin.describeTopics(listOf(topic)).all().get()[topic]?.partitions()
                    ?: return@use false
                val topicPartitions = partitions.map { TopicPartition(topic, it.partition()) }

                if (topicPartitions.isEmpty()) return@use false

                val committed = admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get()
                val endSpecs = topicPartitions.associateWith { OffsetSpec.latest() }
                val endOffsets = admin.listOffsets(endSpecs).all().get()

                var totalLag = 0L
                for (tp in topicPartitions) {
                    val committedOffset = committed[tp]?.offset() ?: 0L
                    val endOffset = endOffsets[tp]?.offset() ?: committedOffset
                    totalLag += maxOf(0L, endOffset - committedOffset)
                }
                totalLag > threshold
            }
        } catch (e: Exception) {
            println("⚠️ Could not check embeddings lag (Kafka Admin): ${e.message}. Allowing fetch-bio.")
            false
        }
    }

    @Serializable
    data class FetchBioRequest(
        val lang: String = "en",
        val limit: Int = 500
    )
    
    @Serializable
    data class FetchBioResponse(
        val status: String,
        val written: Int,
        val enqueued: Int? = null,
        val message: String
    )
    
    suspend fun triggerFetchBio(lang: String = "en", limit: Int = 500): Boolean {
        return try {
            val fetchBioUrl = System.getenv("FETCH_BIO_URL") ?: "http://fetch-bio:8002"
            
            val response = client.post("$fetchBioUrl/fetch-bio") {
                contentType(ContentType.Application.Json)
                setBody(FetchBioRequest(lang = lang, limit = limit))
                timeout {
                    requestTimeoutMillis = 300000 // 5 minutes
                }
                expectSuccess = false
            }
            
            if (!response.status.isSuccess()) {
                val bodyText = runCatching { response.bodyAsText() }.getOrNull()
                println("❌ fetch_bio API failed: ${response.status} ${bodyText ?: ""}".trim())
                return false
            }
            
            val bodyText = response.bodyAsText()
            val parsed = runCatching {
                Json {
                    ignoreUnknownKeys = true
                    isLenient = true
                }.decodeFromString<FetchBioResponse>(bodyText)
            }.getOrElse { err ->
                println("❌ fetch_bio API response parse error: ${err.message}")
                return false
            }
            
            if (parsed.status == "ok") {
                val enq = parsed.enqueued ?: 0
                println("✅ Fetched ${parsed.written} Wikipedia bios" + (if (enq > 0) ", enqueued $enq for embeddings" else " (no embedding jobs: text unchanged)") + ": ${parsed.message}")
                true
            } else {
                println("⚠️ fetch_bio API returned status: ${parsed.status}")
                false
            }
        } catch (e: Exception) {
            println("❌ Error calling fetch_bio API: ${e.message}")
            e.printStackTrace()
            false
        }
    }
}

fun main() {
    Config.initialize()
    
    val resolver = QidResolver()
    val resolveLimit = System.getenv("RESOLVE_LIMIT")?.toIntOrNull() ?: 500
    val resolveOnce = System.getenv("RESOLVE_ONCE")?.lowercase() == "true"
    
    println("Resolver started...")
    
    while (true) {
        try {
            kotlinx.coroutines.runBlocking {
                resolver.resolveQids(resolveLimit)

                // Fetch Wikipedia biographies only when: (1) no ingest job is active,
                // and (2) embeddings worker has caught up (lag below threshold).
                // This serializes embeddings production and avoids adding to backlog.
                when {
                    resolver.hasActiveIngestJobs() ->
                        println("⏭ Skipping fetch-bio: ingest job(s) active")
                    resolver.hasHighEmbeddingsLag() ->
                        println("⏭ Skipping fetch-bio: embeddings topic lag above threshold")
                    else ->
                        resolver.triggerFetchBio("en", resolveLimit)
                }
            }

            if (resolveOnce) {
                println("Resolver finished single batch.")
                break
            }
            
            Thread.sleep(60000) // Wait 1 minute between batches
        } catch (e: Exception) {
            e.printStackTrace()
            Thread.sleep(10000) // Wait 10 seconds on error
        }
    }
}
