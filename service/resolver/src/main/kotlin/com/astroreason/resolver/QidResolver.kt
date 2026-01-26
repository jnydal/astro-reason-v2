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
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.decodeFromString
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import java.io.File
import java.util.concurrent.TimeUnit
import java.time.format.DateTimeFormatter
import java.util.*

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
    private val client = HttpClient(CIO) {
        install(ContentNegotiation) {
            json(Json {
                ignoreUnknownKeys = true
                isLenient = true
            })
        }
        install(HttpTimeout)
    }
    
    suspend fun searchQid(name: String): List<WikidataItem> {
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
            val response = client.get("https://www.wikidata.org/wiki/Special:EntityData/$qid.json") {
                timeout {
                    requestTimeoutMillis = 20000
                }
            }.body<WikidataEntityData>()
            
            val entity = response.entities[qid] ?: return false
            val birthDateClaim = entity.claims["P569"]?.firstOrNull() ?: return false
            val timeValue = birthDateClaim.mainsnak?.datavalue?.value?.get("time") ?: return false
            
            dobIso in timeValue
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
            val candidates = searchQid(person.fullName)
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
                    BioText.insert {
                        it[BioText.personId] = personId
                        it[BioText.revId] = 0L
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
    
    println("Resolver started...")
    
    while (true) {
        try {
            kotlinx.coroutines.runBlocking {
                resolver.resolveQids(500)
                
                // After resolving QIDs, fetch Wikipedia biographies via HTTP API
                resolver.triggerFetchBio("en", 500)
            }
            
            Thread.sleep(60000) // Wait 1 minute between batches
        } catch (e: Exception) {
            e.printStackTrace()
            Thread.sleep(10000) // Wait 10 seconds on error
        }
    }
}
