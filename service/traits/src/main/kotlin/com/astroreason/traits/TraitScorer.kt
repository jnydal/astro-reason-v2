package com.astroreason.traits

import com.astroreason.core.Config
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.HttpTimeout
import io.ktor.client.plugins.timeout
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.request.*
import io.ktor.client.statement.bodyAsText
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.security.MessageDigest

@Serializable
data class VectorScores(
    val sound: Int,
    val visual: Int,
    val oral: Int,
    val anal: Int,
    val urethral: Int,
    val skin: Int,
    val muscular: Int,
    val olfactory: Int
)

@Serializable
data class VectorRationale(
    val sound: String,
    val visual: String,
    val oral: String,
    val anal: String,
    val urethral: String,
    val skin: String,
    val muscular: String,
    val olfactory: String
)

@Serializable
data class TraitResponse(
    val vectors: VectorScores,
    val dominant: List<String>,
    val rationale: VectorRationale,
    val confidence: Double
)

@Serializable
data class OllamaChatRequest(
    val model: String,
    val messages: List<ChatMessage>,
    val options: OllamaOptions = OllamaOptions(),
    val stream: Boolean = false
)

@Serializable
data class ChatMessage(
    val role: String,
    val content: String
)

@Serializable
data class OllamaChatResponse(
    val message: ChatMessage? = null,
    val response: String? = null
)

@Serializable
data class OllamaOptions(
    val temperature: Double = 0.1,
    val num_ctx: Int = 4096,
    val repeat_penalty: Double = 1.05
)

@Serializable
data class OllamaGenerateRequest(
    val model: String,
    val prompt: String,
    val options: OllamaOptions = OllamaOptions(),
    val stream: Boolean = false
)

class TraitScorer(
    private val baseUrl: String,
    val model: String
) {
    private val client = HttpClient(CIO) {
        install(ContentNegotiation) {
            json(Json {
                ignoreUnknownKeys = true
                isLenient = true
            })
        }
        install(HttpTimeout)
    }
    
    private val systemPrompt = """
        You analyze biographies using Yuri Burlan's System-Vector Psychology. 
        Score each of the 8 vectors on a 1..7 scale based ONLY on the biography content. 
        If evidence is weak, use 4 and state 'insufficient evidence' in rationale. 
        Return strict JSON that matches the provided schema. No extra text.
    """.trimIndent()
    
    fun buildVectorPrompt(bioText: String): String {
        return """
Vectors to score (1..7):
- sound, visual, oral, anal, urethral, skin, muscular, olfactory

Scoring rules:
- Base scores only on the biography below. Do not use outside knowledge.
- If evidence is unclear for a vector, assign 4 and add rationale: "insufficient evidence".
- Identify 2-3 dominant vectors by highest scores (ties allowed).
- Provide a brief one-sentence rationale per vector citing concrete biographical cues.

Output JSON schema:
{
  "vectors": {
    "sound": int, "visual": int, "oral": int, "anal": int,
    "urethral": int, "skin": int, "muscular": int, "olfactory": int
  },
  "dominant": [str],        # top 2–3 vector names by score
  "rationale": {           # one sentence per vector
    "sound": str, "visual": str, "oral": str, "anal": str,
    "urethral": str, "skin": str, "muscular": str, "olfactory": str
  },
  "confidence": float       # 0.0..1.0 subjective confidence from evidence quality
}

Biography:
<<<BIO_START>>>
$bioText
<<<BIO_END>>>
Return only JSON.
""".trimIndent()
    }
    
    suspend fun scoreVectorsBio(bioText: String): TraitResponse {
        val messages = listOf(
            ChatMessage(role = "system", content = systemPrompt),
            ChatMessage(role = "user", content = buildVectorPrompt(bioText))
        )
        
        val options = OllamaOptions()
        
        // Try /api/chat first
        val response = try {
            val httpResponse = client.post("$baseUrl/api/chat") {
                contentType(ContentType.Application.Json)
                setBody(OllamaChatRequest(
                    model = model,
                    messages = messages,
                    options = options,
                    stream = false
                ))
                timeout {
                    requestTimeoutMillis = 600000
                }
            }
            if (!httpResponse.status.isSuccess()) {
                throw IllegalStateException("Ollama chat failed: ${httpResponse.status}")
            }
            httpResponse.body<OllamaChatResponse>()
        } catch (e: Exception) {
            // Fallback to /api/generate
            val userParts = messages.filter { it.role == "user" }.joinToString("\n\n") { it.content }
            val prompt = "$systemPrompt\n\n$userParts"
            
            val generateResponseText = client.post("$baseUrl/api/generate") {
                contentType(ContentType.Application.Json)
                setBody(OllamaGenerateRequest(
                    model = model,
                    prompt = prompt,
                    options = options,
                    stream = false
                ))
                timeout {
                    requestTimeoutMillis = 600000
                }
            }.bodyAsText()

            OllamaChatResponse(response = extractGenerateResponse(generateResponseText))
        }
        
        val content = response.message?.content ?: response.response ?: 
            throw IllegalStateException("No response from LLM")
        
        return try {
            Json.decodeFromString<TraitResponse>(content)
        } catch (e: Exception) {
            // Retry with strict JSON instruction
            val retryMessages = messages + ChatMessage(
                role = "system",
                content = "Your last output was not valid JSON. Return strict JSON matching the schema only."
            )
            
            val retryResponse = client.post("$baseUrl/api/chat") {
                contentType(ContentType.Application.Json)
                setBody(OllamaChatRequest(
                    model = model,
                    messages = retryMessages,
                    options = options,
                    stream = false
                ))
                timeout {
                    requestTimeoutMillis = 600000
                }
            }.body<OllamaChatResponse>()
            
            val retryContent = retryResponse.message?.content ?: retryResponse.response ?:
                throw IllegalStateException("No response from LLM on retry")
            
            Json.decodeFromString<TraitResponse>(retryContent)
        }
    }
    
    fun hashPrompt(bioText: String): String {
        val prompt = buildVectorPrompt(bioText)
        val digest = MessageDigest.getInstance("SHA-256")
        val hash = digest.digest(prompt.toByteArray())
        return hash.joinToString("") { "%02x".format(it) }
    }

    private fun extractGenerateResponse(raw: String): String? {
        val trimmed = raw.trim()
        if (trimmed.isEmpty()) return null

        val builder = StringBuilder()
        val lines = trimmed.lines().filter { it.isNotBlank() }
        for (line in lines) {
            try {
                val jsonObj = Json.parseToJsonElement(line).jsonObject
                val chunk = jsonObj["response"]?.jsonPrimitive?.content
                if (chunk != null) {
                    builder.append(chunk)
                }
                val done = jsonObj["done"]?.jsonPrimitive?.content?.toBooleanStrictOrNull() == true
                if (done) break
            } catch (_: Exception) {
                // Ignore malformed lines; rely on valid chunks
            }
        }
        return builder.toString().ifBlank { null }
    }
}
