package com.astroreason.core.config

import com.astroreason.core.loadSettings
import com.astroreason.core.Settings

object Config {
    val settings: Settings = loadSettings()
    
    fun initialize() {
        // Initialize database
        com.astroreason.core.DatabaseManager.initialize(settings.pgDsn)
    }
}
