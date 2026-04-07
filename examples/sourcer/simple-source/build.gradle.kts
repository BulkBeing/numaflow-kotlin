plugins {
    kotlin("jvm")
    application
}

dependencies {
    implementation(rootProject)
    implementation("ch.qos.logback:logback-classic:1.5.18")
}

kotlin {
    jvmToolchain(21)
}

application {
    mainClass.set("SimpleSourceKt")
}
