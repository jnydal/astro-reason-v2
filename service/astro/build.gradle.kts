plugins {
    kotlin("jvm")
    kotlin("plugin.serialization")
    application
}

application {
    mainClass.set("com.astroreason.astro.AstroFeaturesKt")
}

dependencies {
    implementation(project(":service:core"))

    // Swiss Ephemeris - Java wrapper (uses native library under the hood)
    // NOTE: the JAR bundles JNI; no extra Gradle configuration is required,
    // but the ephemeris data files must be available at SE_EPHE_PATH/SWEPH_EPHE_PATH.
    implementation("org.swisseph:swisseph:2.10.03.02")
    
    // Fallback astronomy library
    implementation("org.shredzone.commons:commons-suncalc:3.7")
    
    // Logging
    implementation("ch.qos.logback:logback-classic:1.4.14")
    
    // Testing
    testImplementation("io.kotest:kotest-runner-junit5:5.8.0")
    testImplementation("io.kotest:kotest-assertions-core:5.8.0")
}
