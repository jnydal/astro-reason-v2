rootProject.name = "astro-reason"

fun includeIfPresent(projectPath: String, projectDir: String) {
    if (file(projectDir).isDirectory) {
        include(projectPath)
    }
}

includeIfPresent(":service:core", "service/core")
includeIfPresent(":service:api", "service/api")
includeIfPresent(":service:worker-ingest", "service/worker-ingest")
includeIfPresent(":service:astro", "service/astro")
includeIfPresent(":service:traits", "service/traits")
includeIfPresent(":service:resolver", "service/resolver")
