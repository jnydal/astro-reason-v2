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

    // Fallback astronomy library
    implementation("org.shredzone.commons:commons-suncalc:3.7")
    
    // Logging
    implementation("ch.qos.logback:logback-classic:1.4.14")
    
    // Testing
    testImplementation(kotlin("test"))
    testImplementation("io.kotest:kotest-runner-junit5:5.8.0")
    testImplementation("io.kotest:kotest-assertions-core:5.8.0")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.10.1")
}
