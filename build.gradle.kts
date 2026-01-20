plugins {
    kotlin("jvm") version "1.9.22" apply false
    kotlin("plugin.serialization") version "1.9.22" apply false
}

allprojects {
    group = "com.astroreason"
    version = "0.1.0"

    repositories {
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "org.jetbrains.kotlin.plugin.serialization")

    extensions.configure<org.jetbrains.kotlin.gradle.dsl.KotlinJvmProjectExtension> {
        jvmToolchain(17)
    }

    dependencies {
        add("implementation", "org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.3")
        add("implementation", "org.jetbrains.kotlinx:kotlinx-serialization-json:1.6.2")
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
    }
}
