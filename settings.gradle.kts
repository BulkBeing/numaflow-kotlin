pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "numaflow-kotlin"

include("examples:sinker:simple-sink")
include("examples:sinker:concurrent-sink")
include("examples:sinker:onsuccess-sink")
