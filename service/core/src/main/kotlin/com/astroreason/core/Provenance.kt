package com.astroreason.core

import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import kotlinx.serialization.json.*
import java.time.Instant
import java.util.*

object ProvenanceEvents : Table("provenance_event") {
    val id = long("id").autoIncrement()
    val personId = uuid("person_id").nullable()
    val stage = text("stage")
    val detail = text("detail") // JSONB stored as text
    val createdAt = timestamp("created_at").defaultExpression(CurrentTimestamp())

    override val primaryKey = PrimaryKey(id)
}

data class ProvenanceEvent(
    val id: Long? = null,
    val personId: UUID? = null,
    val stage: String,
    val detail: Map<String, Any>,
    val createdAt: Instant? = null
)

fun logProvenanceEvent(
    personId: UUID? = null,
    stage: String,
    detail: Map<String, Any> = emptyMap()
) {
    transaction(DatabaseManager.getDatabase()) {
        val json = Json { ignoreUnknownKeys = true }
        val detailJson = json.encodeToString(
            detail.mapValues { it.value.toString() }
        )
        
        ProvenanceEvents.insert {
            it[ProvenanceEvents.personId] = personId
            it[ProvenanceEvents.stage] = stage
            it[ProvenanceEvents.detail] = detailJson
        }
    }
}
