plugins {
    kotlin("jvm")
    kotlin("plugin.serialization")
    application
}

application {
    mainClass.set("com.astroreason.resolver.QidResolverKt")
}

dependencies {
    implementation(project(":service:core"))
    
    // Ktor HTTP client
    implementation("io.ktor:ktor-client-core:2.3.7")
    implementation("io.ktor:ktor-client-cio:2.3.7")
    implementation("io.ktor:ktor-client-content-negotiation:2.3.7")
    implementation("io.ktor:ktor-serialization-kotlinx-json:2.3.7")
    
    // Logging
    implementation("ch.qos.logback:logback-classic:1.4.14")
}
