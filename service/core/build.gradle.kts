plugins {
    kotlin("jvm")
    kotlin("plugin.serialization")
}

dependencies {
    // Database
    api("org.jetbrains.exposed:exposed-core:0.49.0")
    api("org.jetbrains.exposed:exposed-dao:0.49.0")
    api("org.jetbrains.exposed:exposed-jdbc:0.49.0")
    api("org.jetbrains.exposed:exposed-kotlin-datetime:0.49.0")
    api("org.jetbrains.exposed:exposed-java-time:0.49.0")
    implementation("com.zaxxer:HikariCP:5.1.0")
    implementation("org.postgresql:postgresql:42.7.1")

    // Kafka
    implementation("org.springframework.kafka:spring-kafka:3.3.5")

    // Logging
    implementation("org.slf4j:slf4j-api:2.0.9")
    implementation("ch.qos.logback:logback-classic:1.4.14")
}
