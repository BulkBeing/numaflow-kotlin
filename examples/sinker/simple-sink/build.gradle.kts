plugins {
    kotlin("jvm")
    application
}

dependencies {
    implementation(rootProject)
}

kotlin {
    jvmToolchain(11)
}

application {
    mainClass.set("SimpleSinkKt")
}

tasks.jar {
    manifest {
        attributes("Main-Class" to "SimpleSinkKt")
    }
    // Create fat jar for Docker
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
}
