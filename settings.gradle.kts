pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
    id("com.gradleup.nmcp.settings") version "1.4.4"
}

nmcpSettings {
    centralPortal {
        username = System.getenv("MVN_CENTRAL_USERNAME")
        password = System.getenv("MVN_CENTRAL_PASSWORD")
        publishingType = "USER_MANAGED"
    }
}

rootProject.name = "numaflow-kotlin"

include("examples:sinker:simple-sink")
include("examples:sinker:concurrent-sink")
include("examples:sinker:onsuccess-sink")

include("examples:mapper:simple-map")
include("examples:mapper:flatmap")
include("examples:mapstreamer:word-splitter")
include("examples:batchmapper:simple-batch")

include("examples:sourcetransformer:event-time-filter")
