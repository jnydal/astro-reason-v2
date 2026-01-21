package com.astroreason.api

import com.astroreason.api.models.*
import com.astroreason.api.storage.createS3Storage
import com.astroreason.api.jobs.ApiJobQueue
import com.astroreason.api.stats.*
import com.astroreason.core.Config
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.plugins.cors.routing.*
import io.ktor.server.request.*
import io.ktor.http.content.PartData
import io.ktor.http.content.forEachPart
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.utils.io.core.readBytes
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
                    is PartData.FileItem -> {
                        val contentType = part.contentType
                        val originalFileName = part.originalFileName
                        if (contentType?.match(ContentType.Application.Xml) == true ||
                            contentType?.match(ContentType.Text.Xml) == true ||
                            originalFileName?.endsWith(".xml", ignoreCase = true) == true) {
                            filename = originalFileName
                            xmlFile = part.provider().readBytes()
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

        route("/stats") {
            get("/correlation") {
                val limit = call.request.queryParameters["limit"]?.toIntOrNull()
                val minSamples = call.request.queryParameters["minSamples"]?.toIntOrNull() ?: 3
                val rows = loadNlpAstroRows(limit)
                call.respond(buildCorrelationResponse(rows, minSamples))
            }

            get("/feature-importance") {
                val limit = call.request.queryParameters["limit"]?.toIntOrNull()
                val minSamples = call.request.queryParameters["minSamples"]?.toIntOrNull() ?: 3
                val rows = loadNlpAstroRows(limit)
                call.respond(buildFeatureImportance(rows, minSamples))
            }

            get("/clusters") {
                val limit = call.request.queryParameters["limit"]?.toIntOrNull()
                val k = call.request.queryParameters["k"]?.toIntOrNull() ?: 5
                val model = call.request.queryParameters["model"]
                if (k < 2) {
                    call.respond(HttpStatusCode.BadRequest, "k must be >= 2")
                    return@get
                }

                val rows = loadEmbeddingAstroRows(limit, model)
                if (rows.isEmpty()) {
                    call.respond(
                        ClusterResponse(
                            k = k,
                            n = 0,
                            embeddingDim = 0,
                            astroFeatureOrder = emptyList(),
                            assignments = emptyList(),
                            centroids = emptyList()
                        )
                    )
                    return@get
                }

                val astroFeatureOrder = rows
                    .map { it.astro.keys.toSet() }
                    .reduce { acc, keys -> acc.intersect(keys) }
                    .sorted()

                if (astroFeatureOrder.isEmpty()) {
                    call.respond(HttpStatusCode.BadRequest, "No shared astro features available")
                    return@get
                }

                val eligible = rows.filter { row -> astroFeatureOrder.all { it in row.astro } }
                if (eligible.size < k) {
                    call.respond(HttpStatusCode.BadRequest, "Not enough rows for k=$k")
                    return@get
                }

                call.respond(buildClusterResponse(eligible, k, astroFeatureOrder))
            }

            get("/export") {
                val limit = call.request.queryParameters["limit"]?.toIntOrNull()
                val format = call.request.queryParameters["format"]?.lowercase() ?: "json"
                val clusterK = call.request.queryParameters["clusterK"]?.toIntOrNull()
                val clusterModel = call.request.queryParameters["clusterModel"]
                val rows = loadNlpAstroRows(limit)

                val clusterAssignments = if (clusterK != null && clusterK >= 2) {
                    val clusterRows = loadEmbeddingAstroRows(limit, clusterModel)
                    val astroFeatureOrder = clusterRows
                        .map { it.astro.keys.toSet() }
                        .reduceOrNull { acc, keys -> acc.intersect(keys) }
                        ?.sorted()
                        ?: emptyList()

                    if (astroFeatureOrder.isNotEmpty() && clusterRows.size >= clusterK) {
                        val response = buildClusterResponse(clusterRows, clusterK, astroFeatureOrder)
                        response.assignments.associate { UUID.fromString(it.personId) to it.cluster }
                    } else {
                        emptyMap()
                    }
                } else {
                    emptyMap()
                }

                val export = buildExportResponse(rows, clusterAssignments)

                if (format == "csv") {
                    val csv = buildExportCsv(export, includeClusters = clusterAssignments.isNotEmpty())
                    call.respondText(csv, ContentType.Text.CSV)
                } else {
                    call.respond(export)
                }
            }
        }
    }
}
