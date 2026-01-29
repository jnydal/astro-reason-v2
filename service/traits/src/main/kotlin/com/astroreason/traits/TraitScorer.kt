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
import kotlinx.serialization.json.jsonArray
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

@Serializable
data class OpenAiChatRequest(
    val model: String,
    val messages: List<OpenAiChatMessage>,
    val temperature: Double = 0.1,
    val stream: Boolean = false
)

@Serializable
data class OpenAiChatMessage(
    val role: String,
    val content: String
)

@Serializable
data class OpenAiCompletionRequest(
    val model: String,
    val prompt: String,
    val temperature: Double = 0.1
)

@Serializable
data class OpenAiChatChoice(
    val message: OpenAiChatMessage? = null,
    val text: String? = null
)

@Serializable
data class OpenAiChatResponse(
    val choices: List<OpenAiChatChoice> = emptyList()
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
    private val apiKey = System.getenv("OPENAI_API_KEY") ?: System.getenv("LLM_API_KEY")
    private val trimmedBaseUrl = baseUrl.trimEnd('/')
    private val ollamaBaseUrl = if (trimmedBaseUrl.endsWith("/api")) trimmedBaseUrl else "$trimmedBaseUrl/api"
    private val openAiBaseUrl = if (trimmedBaseUrl.endsWith("/v1")) trimmedBaseUrl else "$trimmedBaseUrl/v1"
    
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
        val content = fetchContent(messages)
        
        return try {
            Json.decodeFromString<TraitResponse>(content)
        } catch (e: Exception) {
            // Retry with strict JSON instruction
            val retryMessages = messages + ChatMessage(
                role = "system",
                content = "Your last output was not valid JSON. Return strict JSON matching the schema only."
            )
            val retryContent = fetchContent(retryMessages)
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

    private fun extractChatResponse(raw: String): String {
        val trimmed = raw.trim()
        if (trimmed.isEmpty()) return ""

        val builder = StringBuilder()
        val lines = trimmed.lines().filter { it.isNotBlank() }
        for (line in lines) {
            try {
                val jsonObj = Json.parseToJsonElement(line).jsonObject
                val message = jsonObj["message"]?.jsonObject
                val chunk = message?.get("content")?.jsonPrimitive?.content
                if (chunk != null) {
                    builder.append(chunk)
                }
                val done = jsonObj["done"]?.jsonPrimitive?.content?.toBooleanStrictOrNull() == true
                if (done) break
            } catch (_: Exception) {
                // Ignore malformed lines; rely on valid chunks
            }
        }
        return builder.toString()
    }

    private fun extractOpenAiResponse(raw: String): String? {
        val trimmed = raw.trim()
        if (trimmed.isEmpty()) return null

        val lines = trimmed.lines().map { it.trim() }.filter { it.isNotBlank() }
        if (lines.any { it.startsWith("data:") }) {
            val builder = StringBuilder()
            for (line in lines) {
                if (!line.startsWith("data:")) continue
                val payload = line.removePrefix("data:").trim()
                if (payload == "[DONE]") break
                try {
                    val jsonObj = Json.parseToJsonElement(payload).jsonObject
                    val choice = jsonObj["choices"]?.jsonArray?.firstOrNull()?.jsonObject
                    val delta = choice?.get("delta")?.jsonObject
                    val message = choice?.get("message")?.jsonObject
                    val chunk = delta?.get("content")?.jsonPrimitive?.content
                        ?: message?.get("content")?.jsonPrimitive?.content
                        ?: choice?.get("text")?.jsonPrimitive?.content
                    if (chunk != null) {
                        builder.append(chunk)
                    }
                } catch (_: Exception) {
                    // Ignore malformed lines; rely on valid chunks
                }
            }
            return builder.toString().ifBlank { null }
        }

        return try {
            val parsed = Json.decodeFromString<OpenAiChatResponse>(trimmed)
            parsed.choices.firstOrNull()?.message?.content ?: parsed.choices.firstOrNull()?.text
        } catch (_: Exception) {
            null
        }
    }

    private suspend fun fetchContent(messages: List<ChatMessage>): String {
        val options = OllamaOptions()
        val errors = mutableListOf<String>()

        suspend fun attempt(label: String, block: suspend () -> String?): String? {
            return try {
                val result = block()
                if (result.isNullOrBlank()) {
                    errors.add("$label returned empty response")
                    null
                } else {
                    result
                }
            } catch (e: Exception) {
                val message = e.message ?: e.javaClass.simpleName
                errors.add("$label failed: $message")
                null
            }
        }

        val userParts = messages.filter { it.role == "user" }.joinToString("\n\n") { it.content }
        val prompt = "$systemPrompt\n\n$userParts"

        return attempt("ollama /api/chat") {
            val httpResponse = client.post("$ollamaBaseUrl/chat") {
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
                throw IllegalStateException("status ${httpResponse.status}")
            }
            val responseType = httpResponse.contentType()?.withoutParameters()
            if (responseType == ContentType.Application.Json) {
                val parsed = httpResponse.body<OllamaChatResponse>()
                parsed.message?.content ?: parsed.response
            } else {
                val raw = httpResponse.bodyAsText()
                extractChatResponse(raw)
            }
        } ?: attempt("ollama /api/generate") {
            val httpResponse = client.post("$ollamaBaseUrl/generate") {
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
            }
            if (!httpResponse.status.isSuccess()) {
                throw IllegalStateException("status ${httpResponse.status}")
            }
            val generateResponseText = httpResponse.bodyAsText()
            extractGenerateResponse(generateResponseText)
        } ?: attempt("openai /v1/chat/completions") {
            val httpResponse = client.post("$openAiBaseUrl/chat/completions") {
                contentType(ContentType.Application.Json)
                apiKey?.let { header(HttpHeaders.Authorization, "Bearer $it") }
                setBody(OpenAiChatRequest(
                    model = model,
                    messages = messages.map { OpenAiChatMessage(it.role, it.content) },
                    temperature = options.temperature,
                    stream = false
                ))
                timeout {
                    requestTimeoutMillis = 600000
                }
            }
            if (!httpResponse.status.isSuccess()) {
                throw IllegalStateException("status ${httpResponse.status}")
            }
            val raw = httpResponse.bodyAsText()
            extractOpenAiResponse(raw)
        } ?: attempt("openai /v1/completions") {
            val httpResponse = client.post("$openAiBaseUrl/completions") {
                contentType(ContentType.Application.Json)
                apiKey?.let { header(HttpHeaders.Authorization, "Bearer $it") }
                setBody(OpenAiCompletionRequest(
                    model = model,
                    prompt = prompt,
                    temperature = options.temperature
                ))
                timeout {
                    requestTimeoutMillis = 600000
                }
            }
            if (!httpResponse.status.isSuccess()) {
                throw IllegalStateException("status ${httpResponse.status}")
            }
            val raw = httpResponse.bodyAsText()
            extractOpenAiResponse(raw)
        } ?: throw IllegalStateException("No response from LLM. Attempts: ${errors.joinToString("; ")}")
    }
}
