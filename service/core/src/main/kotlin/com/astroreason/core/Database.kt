package com.astroreason.core

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.transactions.transaction
import java.sql.Connection

object DatabaseManager {
    private var dataSource: HikariDataSource? = null
    private var exposedDb: Database? = null

    fun initialize(dsn: String) {
        val config = HikariConfig().apply {
            jdbcUrl = dsn.replace("postgresql+psycopg://", "jdbc:postgresql://")
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
