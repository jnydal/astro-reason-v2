package com.astroreason.core

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.transactions.transaction
import java.net.URI
import java.sql.Connection

object DatabaseManager {
    private var dataSource: HikariDataSource? = null
    private var exposedDb: Database? = null

    fun initialize(dsn: String) {
        val normalized = dsn.replace("postgresql+psycopg://", "postgresql://")
        val parsedUri = when {
            normalized.startsWith("jdbc:postgresql://") ->
                URI.create(normalized.removePrefix("jdbc:"))
            normalized.startsWith("postgresql://") ->
                URI.create(normalized)
            else -> null
        }

        val (jdbcUrl, username, password) = if (parsedUri != null && parsedUri.scheme == "postgresql") {
            val userInfo = parsedUri.userInfo?.split(":", limit = 2)
            val user = userInfo?.getOrNull(0)
            val pass = userInfo?.getOrNull(1)

            val host = parsedUri.host
            val port = if (parsedUri.port == -1) null else parsedUri.port
            val dbName = parsedUri.path?.removePrefix("/")?.takeIf { it.isNotBlank() }
            val query = parsedUri.query?.takeIf { it.isNotBlank() }

            val url = buildString {
                append("jdbc:postgresql://")
                append(host)
                if (port != null) append(":").append(port)
                if (dbName != null) append("/").append(dbName)
                if (query != null) append("?").append(query)
            }

            Triple(url, user, pass)
        } else {
            Triple(
                normalized.replace("postgresql://", "jdbc:postgresql://"),
                null,
                null
            )
        }

        val config = HikariConfig().apply {
            this.jdbcUrl = jdbcUrl
            if (!username.isNullOrBlank()) this.username = username
            if (!password.isNullOrBlank()) this.password = password
            driverClassName = "org.postgresql.Driver"
            maximumPoolSize = 10
            minimumIdle = 2
            connectionTimeout = 30000
            idleTimeout = 600000
            maxLifetime = 1800000
            isAutoCommit = false
        }

        dataSource = HikariDataSource(config)
        exposedDb = Database.connect(dataSource!!)
    }

    fun getConnection(): Connection {
        return dataSource?.connection ?: throw IllegalStateException("Database not initialized")
    }

    fun getDatabase(): Database {
        return exposedDb ?: throw IllegalStateException("Database not initialized")
    }

    fun healthCheck(): Boolean {
        return try {
            transaction(getDatabase()) {
                exec("SELECT 1")
            }
            true
        } catch (e: Exception) {
            false
        }
    }

    fun close() {
        dataSource?.close()
        dataSource = null
        exposedDb = null
    }
}
