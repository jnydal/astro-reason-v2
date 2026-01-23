package com.astroreason.core.schema

import org.jetbrains.exposed.dao.id.UUIDTable
import org.jetbrains.exposed.dao.id.LongIdTable
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.javatime.CurrentTimestamp
import org.jetbrains.exposed.sql.javatime.date
import org.jetbrains.exposed.sql.javatime.time
import org.jetbrains.exposed.sql.javatime.timestamp
import java.util.*

// Helper for JSONB columns - stored as text in Exposed
fun Table.jsonb(name: String): Column<String> = text(name)

object PersonRaw : UUIDTable("person_raw", columnName = "id") {
    val xmlId = text("xml_id").nullable()
    val name = text("name")
    val biographyStub = text("biography_stub").nullable()
    val createdAt = timestamp("created_at").defaultExpression(CurrentTimestamp())
    
    init {
        uniqueIndex(xmlId)
    }
}

object Birth : UUIDTable("birth", columnName = "person_id") {
    val date = date("date").nullable()
    val time = time("time").nullable()
    val tz = text("tz").nullable()
    val lat = double("lat").nullable()
    val lon = double("lon").nullable()
    val tzOffsetMinutes = integer("tz_offset_minutes").nullable()
    val placeName = text("place_name").nullable()
    val dataQuality = text("data_quality").nullable()
    
    init {
        id.references(PersonRaw.id, onDelete = ReferenceOption.CASCADE)
    }
}

object EntityLink : UUIDTable("entity_link", columnName = "person_id") {
    val qid = text("qid").nullable()
    val method = text("method").nullable()
    val score = double("score").nullable()
    val candidatesJson = jsonb("candidates_json").nullable()
    val decidedAt = timestamp("decided_at").nullable()
    
    init {
        id.references(PersonRaw.id, onDelete = ReferenceOption.CASCADE)
    }
}

object BioText : Table("bio_text") {
    val personId = uuid("person_id").references(PersonRaw.id, onDelete = ReferenceOption.CASCADE)
    val qid = text("qid").nullable()
    val text = text("text").nullable()
    val textSha256 = text("text_sha256").nullable()
    val lang = text("lang").nullable()
    val wikiPageid = long("wiki_pageid").nullable()
    val revId = long("rev_id").nullable()
    val url = text("url").nullable()
    val license = text("license").nullable()
    val retrievedAt = timestamp("retrieved_at").nullable()
    val charCount = integer("char_count").nullable()
    val textUri = text("text_uri").nullable()
    val sourceCol = text("source").nullable()
    val updatedAt = timestamp("updated_at").nullable()
    val textHash = text("text_hash").nullable()
    
    override val primaryKey = PrimaryKey(personId, revId)
}

object NlpTraits : UUIDTable("nlp_traits", columnName = "person_id") {
    val model = text("model").nullable()
    val version = text("version").nullable()
    val scoresJson = jsonb("scores_json").nullable()
    val rationaleJson = jsonb("rationale_json").nullable()
    val promptHash = text("prompt_hash").nullable()
    val createdAt = timestamp("created_at").defaultExpression(CurrentTimestamp())
    
    init {
        id.references(PersonRaw.id, onDelete = ReferenceOption.CASCADE)
    }
}

object NlpVectors : LongIdTable("nlp_vectors") {
    val personId = uuid("person_id").references(PersonRaw.id, onDelete = ReferenceOption.CASCADE)
    val vectors = jsonb("vectors")
    val dominant = text("dominant") // PostgreSQL array stored as text
    val confidence = double("confidence")
    val modelName = text("model_name")
    val provider = text("provider")
    val temperature = double("temperature")
    val promptHash = text("prompt_hash")
    val createdAt = timestamp("created_at").defaultExpression(CurrentTimestamp())
    
    init {
        check { confidence greaterEq 0.0 and (confidence lessEq 1.0) }
    }
}

object AstroFeatures : UUIDTable("astro_features", columnName = "person_id") {
    val system = text("system")
    val jdUtc = double("jd_utc")
    val unknownTime = bool("unknown_time").default(false)
    val longs = jsonb("longs")
    val houses = jsonb("houses").nullable()
    val aspects = jsonb("aspects")
    val elemRatios = jsonb("elem_ratios")
    val modalityRatios = jsonb("modality_ratios")
    val featureVec = jsonb("feature_vec")
    val createdAt = timestamp("created_at").defaultExpression(CurrentTimestamp())
    
    init {
        id.references(PersonRaw.id, onDelete = ReferenceOption.CASCADE)
    }
}

object Embeddings384 : Table("embeddings_384") {
    val personId = uuid("person_id").references(PersonRaw.id, onDelete = ReferenceOption.CASCADE)
    val modelName = text("model_name")
    val dim = integer("dim").check { it eq 384 }
    val vector = text("vector") // pgvector stored as text, will need custom handling
    val textHash = text("text_hash").nullable()
    val meta = jsonb("meta").nullable()
    val sourceCol = text("source").nullable()
    val updatedAt = timestamp("updated_at").nullable()
    val createdAt = timestamp("created_at").defaultExpression(CurrentTimestamp())
    
    override val primaryKey = PrimaryKey(personId, modelName)
}

object Embeddings768 : Table("embeddings_768") {
    val personId = uuid("person_id").references(PersonRaw.id, onDelete = ReferenceOption.CASCADE)
    val modelName = text("model_name")
    val dim = integer("dim").check { it eq 768 }
    val vector = text("vector") // pgvector stored as text, will need custom handling
    val textHash = text("text_hash").nullable()
    val meta = jsonb("meta").nullable()
    val sourceCol = text("source").nullable()
    val updatedAt = timestamp("updated_at").nullable()
    val createdAt = timestamp("created_at").defaultExpression(CurrentTimestamp())
    
    override val primaryKey = PrimaryKey(personId, modelName)
}

object Embeddings1024 : Table("embeddings_1024") {
    val personId = uuid("person_id").references(PersonRaw.id, onDelete = ReferenceOption.CASCADE)
    val modelName = text("model_name")
    val dim = integer("dim").check { it eq 1024 }
    val vector = text("vector") // pgvector stored as text, will need custom handling
    val textHash = text("text_hash").nullable()
    val meta = jsonb("meta").nullable()
    val sourceCol = text("source").nullable()
    val updatedAt = timestamp("updated_at").nullable()
    val createdAt = timestamp("created_at").defaultExpression(CurrentTimestamp())
    
    override val primaryKey = PrimaryKey(personId, modelName)
}

object Embeddings1536 : Table("embeddings_1536") {
    val personId = uuid("person_id").references(PersonRaw.id, onDelete = ReferenceOption.CASCADE)
    val modelName = text("model_name")
    val dim = integer("dim").check { it eq 1536 }
    val vector = text("vector") // pgvector stored as text, will need custom handling
    val textHash = text("text_hash").nullable()
    val meta = jsonb("meta").nullable()
    val sourceCol = text("source").nullable()
    val updatedAt = timestamp("updated_at").nullable()
    val createdAt = timestamp("created_at").defaultExpression(CurrentTimestamp())

    override val primaryKey = PrimaryKey(personId, modelName)
}

object JobStatusTable : UUIDTable("job_status", columnName = "id") {
    val function = text("function")
    val status = text("status")
    val argsJson = jsonb("args_json")
    val kwargsJson = jsonb("kwargs_json")
    val enqueuedAt = long("enqueued_at")
    val startedAt = long("started_at").nullable()
    val endedAt = long("ended_at").nullable()
    val result = text("result").nullable()
    val excInfo = text("exc_info").nullable()
    val updatedAt = timestamp("updated_at").defaultExpression(CurrentTimestamp())
}
