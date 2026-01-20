package com.astroreason.ingest

import aws.sdk.kotlin.services.s3.S3Client
import aws.sdk.kotlin.services.s3.model.GetObjectRequest
import aws.sdk.kotlin.runtime.auth.credentials.StaticCredentialsProvider
import aws.smithy.kotlin.runtime.auth.awscredentials.Credentials
import aws.smithy.kotlin.runtime.content.writeToFile
import aws.smithy.kotlin.runtime.net.url.Url
import com.astroreason.core.*
import com.astroreason.core.schema.*
import com.astroreason.core.queue.JobQueue
import com.astroreason.core.queue.createJobQueue
import kotlinx.coroutines.runBlocking
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.transactions.transaction
import java.io.File
import java.util.*
import kotlin.io.path.createTempFile
import kotlin.io.path.deleteIfExists

fun parseAdbXml(objectUri: String, meta: Map<String, String>) {
    require(objectUri.startsWith("s3://")) { "Expected s3://bucket/key" }
    
    val (bucket, key) = objectUri.removePrefix("s3://").split("/", limit = 2)
    val source = meta["source"] ?: "astrodb-upload"
    
    val settings = Config.settings
    val s3Client = S3Client {
        settings.s3Endpoint?.let { endpointUrl = Url.parse(it) }
        settings.s3AccessKey?.let {
            credentialsProvider = StaticCredentialsProvider(
                Credentials(it, settings.s3SecretKey ?: "")
            )
        }
        region = "us-east-1"
    }
    
    val tempFile = runBlocking {
        val tempPath = createTempFile(prefix = "adb-", suffix = ".xml")
        s3Client.getObject(GetObjectRequest {
            this.bucket = bucket
            this.key = key
        }) { response ->
            response.body?.writeToFile(tempPath)
        }
        tempPath.toFile()
    }
    
    try {
        val parser = XmlParser()
        val touchedPids = mutableSetOf<UUID>()
        var recordsSeen = 0
        
        transaction(DatabaseManager.getDatabase()) {
            val birthBatch = mutableListOf<Pair<UUID, BirthData>>()
            val bioBatch = mutableListOf<Pair<UUID, BioData>>()
            
            fun flushBatches() {
                if (birthBatch.isNotEmpty()) {
                    for ((personId, data) in birthBatch) {
                        val existing = Birth.select { Birth.id eq personId }.firstOrNull()
                        if (existing != null) {
                            Birth.update({ Birth.id eq personId }) {
                                it[Birth.date] = data.date
                                it[Birth.time] = data.time
                                it[Birth.tzOffsetMinutes] = data.tzOffsetMinutes
                                it[Birth.placeName] = data.placeName
                                it[Birth.lat] = data.lat
                                it[Birth.lon] = data.lon
                                it[Birth.dataQuality] = data.dataQuality
                            }
                        } else {
                            Birth.insert {
                                it[Birth.id] = personId
                                it[Birth.date] = data.date
                                it[Birth.time] = data.time
                                it[Birth.tzOffsetMinutes] = data.tzOffsetMinutes
                                it[Birth.placeName] = data.placeName
                                it[Birth.lat] = data.lat
                                it[Birth.lon] = data.lon
                                it[Birth.dataQuality] = data.dataQuality
                            }
                        }
                    }
                    birthBatch.clear()
                }
                
                if (bioBatch.isNotEmpty()) {
                    for ((personId, data) in bioBatch) {
                        val revId = 0L // Default rev_id
                        val existing = BioText.select { 
                            BioText.personId eq personId and (BioText.revId eq revId)
                        }.firstOrNull()
                        if (existing != null) {
                            BioText.update({ 
                                BioText.personId eq personId and (BioText.revId eq revId)
                            }) {
                                it[BioText.text] = data.text
                                it[BioText.textHash] = data.textHash
                                it[BioText.sourceCol] = data.source
                                it[BioText.updatedAt] = java.time.Instant.now()
                            }
                        } else {
                            BioText.insert {
                                it[BioText.personId] = personId
                                it[BioText.revId] = revId
                                it[BioText.text] = data.text
                                it[BioText.textHash] = data.textHash
                                it[BioText.sourceCol] = data.source
                                it[BioText.updatedAt] = java.time.Instant.now()
                            }
                        }
                    }
                    bioBatch.clear()
                }
            }
            
            for (rec in parser.iterPeople(tempFile.absolutePath)) {
                recordsSeen++
                
                // Upsert person_raw
                val existing = PersonRaw.select { PersonRaw.xmlId eq rec.adbId }.firstOrNull()
                val personId = if (existing != null) {
                    PersonRaw.update({ PersonRaw.xmlId eq rec.adbId }) {
                        it[name] = rec.fullName ?: ""
                        it[biographyStub] = rec.bioText
                    }
                    existing[PersonRaw.id].value
                } else {
                    PersonRaw.insert {
                        it[xmlId] = rec.adbId
                        it[name] = rec.fullName ?: ""
                        it[biographyStub] = rec.bioText
                    }[PersonRaw.id].value
                }
                
                touchedPids.add(personId)
                
                // Birth data
                birthBatch.add(personId to BirthData(
                    personId = personId,
                    date = rec.date?.let { java.time.LocalDate.parse(it) },
                    time = rec.time?.let { java.time.LocalTime.parse(it) },
                    tzOffsetMinutes = tzToMinutes(rec.tz),
                    placeName = rec.place,
                    lat = rec.lat,
                    lon = rec.lon,
                    dataQuality = rec.rating
                ))
                
                // Bio text
                if (!rec.bioText.isNullOrBlank()) {
                    bioBatch.add(personId to BioData(
                        personId = personId,
                        text = rec.bioText,
                        textHash = sha256(rec.bioText),
                        source = source
                    ))
                }
                
                // Periodic flush
                if (birthBatch.size + bioBatch.size >= 500) {
                    flushBatches()
                    commit()
                }
            }
            
            // Final flush
            flushBatches()
            commit()
        }
        
        // Note: semantic embeddings are now triggered after wiki enrichment
        // from the fetch-bio service, to ensure they run on full biographies.
        println("✅ Parsed $recordsSeen records, upserted ${touchedPids.size} people")
        
    } finally {
        tempFile.delete()
    }
}

data class BirthData(
    val personId: UUID,
    val date: java.time.LocalDate?,
    val time: java.time.LocalTime?,
    val tzOffsetMinutes: Int?,
    val placeName: String?,
    val lat: Double?,
    val lon: Double?,
    val dataQuality: String?
)

data class BioData(
    val personId: UUID,
    val text: String,
    val textHash: String,
    val source: String
)
