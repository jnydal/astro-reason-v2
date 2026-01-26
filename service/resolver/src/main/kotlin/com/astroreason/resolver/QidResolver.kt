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
import io.ktor.client.plugins.timeout
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import kotlinx.coroutines.delay
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.decodeFromString
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import java.io.File
import java.util.concurrent.TimeUnit
import java.time.format.DateTimeFormatter
import java.util.*
import kotlin.random.Random

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
    val value: Map<String, String> = emptyMap()
)

class QidResolver {
    private val minIntervalMs = ((System.getenv("WIKIDATA_MIN_INTERVAL_SEC") ?: "1.0").toDouble() * 1000).toLong()
    private val jitterMs = ((System.getenv("WIKIDATA_JITTER_SEC") ?: "0.2").toDouble() * 1000).toLong()
    private var nextRequestTimeMs = 0L

    private val client = HttpClient(CIO) {
        install(ContentNegotiation) {
            json(Json {
                ignoreUnknownKeys = true
                isLenient = true
            })
        }
        install(HttpTimeout)
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
    
    suspend fun dobMatches(qid: String, dobIso: String?): Boolean {
        if (dobIso.isNullOrBlank()) return false
        
        return try {
            waitForRateLimit()
            val response = client.get("https://www.wikidata.org/wiki/Special:EntityData/$qid.json") {
                timeout {
                    requestTimeoutMillis = 20000
                }
            }.body<WikidataEntityData>()
            
            val entity = response.entities[qid] ?: return false
            val birthDateClaim = entity.claims["P569"]?.firstOrNull() ?: return false
            val timeValue = birthDateClaim.mainsnak?.datavalue?.value?.get("time") ?: return false
            
            dobIso == extractWikidataDate(timeValue)
        } catch (e: Exception) {
            false
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
                .leftJoin(BioText, { PersonRaw.id }, { BioText.personId })
                .slice(PersonRaw.id, PersonRaw.name, Birth.date)
                .select { BioText.qid.isNull() or (BioText.qid eq "") }
                .limit(limit)
                .map { row ->
                    PendingPerson(
                        personId = row[PersonRaw.id].value,
                        fullName = row[PersonRaw.name],
                        dobIso = row[Birth.date]?.format(DateTimeFormatter.ISO_DATE)
                    )
                }
        }

        val resolved = mutableListOf<Pair<UUID, String>>()

        for (person in pending) {
            var candidates = searchQid(normalizeName(person.fullName))
            if (candidates.isEmpty()) {
                candidates = searchQid(person.fullName)
            }
            var qid: String? = null

            // Try to match by date
            for (candidate in candidates.take(10)) {
                if (dobMatches(candidate.id, person.dobIso)) {
                    qid = candidate.id
                    break
                }
            }

            // Fallback to first candidate
            if (qid == null && candidates.isNotEmpty()) {
                qid = candidates[0].id
            }

            if (qid != null) {
                resolved.add(person.personId to qid)
            }
        }

        if (resolved.isNotEmpty()) {
            transaction(DatabaseManager.getDatabase()) {
                for ((personId, qid) in resolved) {
                    BioText.insertIgnore {
                        it[BioText.personId] = personId
                        it[BioText.revId] = 0L
                        it[BioText.qid] = qid
                    }
                    BioText.update({ (BioText.personId eq personId) and (BioText.revId eq 0L) }) {
                        it[BioText.qid] = qid
                    }
                }
            }
        }

        println("✅ Resolved ${resolved.size} QIDs")
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
                println("✅ Fetched ${parsed.written} Wikipedia bios: ${parsed.message}")
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
                
                // After resolving QIDs, fetch Wikipedia biographies via HTTP API
                resolver.triggerFetchBio("en", resolveLimit)
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
