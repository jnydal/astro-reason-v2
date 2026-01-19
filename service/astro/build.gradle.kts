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
    
    // Swiss Ephemeris - using Java wrapper if available
    // Note: Actual JNI library needs to be set up separately
    // implementation("org.swisseph:swisseph:2.10.03.02")
    
    // Fallback astronomy library
    implementation("org.shredzone.commons:commons-suncalc:3.7")
    
    // Logging
    implementation("ch.qos.logback:logback-classic:1.4.14")
    
    // Testing
    testImplementation("io.kotest:kotest-runner-junit5:5.8.0")
    testImplementation("io.kotest:kotest-assertions-core:5.8.0")
}
