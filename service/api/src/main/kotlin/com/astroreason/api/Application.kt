package com.astroreason.api

import com.astroreason.api.models.*
import com.astroreason.api.storage.createS3Storage
import com.astroreason.api.jobs.ApiJobQueue
import com.astroreason.core.Config
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.plugins.cors.routing.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import kotlinx.serialization.json.Json
import java.util.*

fun main(args: Array<String>) {
    Config.initialize()
    
    embeddedServer(Netty, port = 8000, host = "0.0.0.0", module = Application::module)
        .start(wait = true)
}

fun Application.module() {
    val storage = createS3Storage()
    val jobQueue = ApiJobQueue()
    
    // Ensure bucket exists on startup
    storage.ensureBucket()
    
    install(ContentNegotiation) {
        json(Json {
            prettyPrint = true
            isLenient = true
            ignoreUnknownKeys = true
        })
    }
    
    install(CORS) {
        allowMethod(HttpMethod.Options)
        allowMethod(HttpMethod.Get)
        allowMethod(HttpMethod.Post)
        allowHeader(HttpHeaders.ContentType)
        anyHost()
    }
    
    routing {
        get("/healthz") {
            call.respond(HealthResponse())
        }
        
        get("/version") {
            call.respond(VersionInfo())
        }
        
        post("/ingest/astrodatabank") {
            val multipart = call.receiveMultipart()
            var xmlFile: ByteArray? = null
            var filename: String? = null
            
            multipart.forEachPart { part ->
                when (part) {
                    is io.ktor.server.request.ApplicationPart.FileItem -> {
                        val contentType = part.contentType
                        val originalFileName = part.originalFileName
                        if (contentType?.match(ContentType.Application.Xml) == true ||
                            contentType?.match(ContentType.Text.Xml) == true ||
                            originalFileName?.endsWith(".xml", ignoreCase = true) == true) {
                            filename = originalFileName
                            xmlFile = part.streamProvider().readBytes()
                        }
                    }
                    else -> {}
                }
                part.dispose()
            }
            
            if (xmlFile == null || xmlFile!!.isEmpty()) {
                call.respond(HttpStatusCode.BadRequest, "Expected an .xml file")
                return@post
            }
            
            // Light sanity check
            val contentStr = String(xmlFile!!)
            if (!contentStr.contains("<astrodatabank", ignoreCase = true) &&
                !contentStr.contains("<AstroDatabank", ignoreCase = true)) {
                // Allow anyway, worker can fail with better diagnostics
            }
            
            val objectUri = storage.putBytes("adb-uploads", xmlFile!!, "application/xml")
            val job = jobQueue.enqueueParseAdbXml(objectUri, "astrodb-upload")
            
            call.respond(IngestResponse(jobId = job.id, objectUri = objectUri))
        }
        
        get("/jobs/{jobId}") {
            val jobId = call.parameters["jobId"] ?: run {
                call.respond(HttpStatusCode.BadRequest, "Missing job_id")
                return@get
            }
            
            val job = jobQueue.getJobStatus(jobId) ?: run {
                call.respond(HttpStatusCode.NotFound, "Job not found")
                return@get
            }
            
            call.respond(JobStatusResponse(
                id = job.id,
                status = job.status.name.lowercase(),
                enqueuedAt = job.enqueuedAt.toString(),
                startedAt = job.startedAt?.toString(),
                endedAt = job.endedAt?.toString(),
                excInfo = job.excInfo,
                result = job.result
            ))
        }
    }
}
