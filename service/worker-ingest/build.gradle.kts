plugins {
    kotlin("jvm")
    kotlin("plugin.serialization")
    application
}

application {
    mainClass.set("com.astroreason.ingest.IngestWorkerKt")
}

dependencies {
    implementation(project(":service:core"))
    
    // AWS SDK
    implementation("aws.sdk.kotlin:s3:1.0.21")
    
    // XML parsing
    implementation("javax.xml.stream:stax-api:1.0-2")
    
    // Logging
    implementation("ch.qos.logback:logback-classic:1.4.14")
}
