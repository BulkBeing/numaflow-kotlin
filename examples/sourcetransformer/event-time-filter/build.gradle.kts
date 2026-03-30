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
    mainClass.set("EventTimeFilterKt")
}

tasks.jar {
    manifest {
        attributes("Main-Class" to "EventTimeFilterKt")
    }
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
}
