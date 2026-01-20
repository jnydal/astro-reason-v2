package com.astroreason.core

import com.astroreason.core.config.Config as CoreConfig

object Config {
    val settings = CoreConfig.settings

    fun initialize() {
        CoreConfig.initialize()
    }
}
