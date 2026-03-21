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
    mainClass.set("ConcurrentSinkKt")
}

tasks.jar {
    manifest {
        attributes("Main-Class" to "ConcurrentSinkKt")
    }
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
}
