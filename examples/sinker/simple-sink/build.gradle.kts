plugins {
    kotlin("jvm")
    application
}

dependencies {
    implementation(rootProject)
    implementation("ch.qos.logback:logback-classic:1.5.18")
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
