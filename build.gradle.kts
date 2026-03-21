plugins {
    kotlin("jvm") version "2.3.0"
    id("org.jetbrains.dokka") version "2.1.0"
}

group = "io.numaproj.numaflowkt"
version = "0.1.0-SNAPSHOT"
description = "Kotlin-idiomatic SDK for Numaflow User-Defined Functions"

allprojects {
    repositories {
        mavenLocal()
        mavenCentral()
    }
}

// gRPC and protobuf versions are pinned to match numaflow-java's transitive
// dependencies. Bumping them independently would cause runtime classpath conflicts.
val grpcVersion = "1.72.0"
val grpcKotlinVersion = "1.4.3"
val protobufVersion = "4.31.0"
val coroutinesVersion = "1.10.2"

dependencies {
    // Java SDK — implementation-only so it never leaks to consumers
    implementation("io.numaproj.numaflow:numaflow-java:0.11.0")

    // Coroutines — exposed to consumers (Flow, suspend in API)
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:$coroutinesVersion")

    // gRPC Kotlin stubs for TestKit client
    implementation("io.grpc:grpc-kotlin-stub:$grpcKotlinVersion")
    implementation("io.grpc:grpc-stub:$grpcVersion")
    implementation("io.grpc:grpc-protobuf:$grpcVersion")
    implementation("io.grpc:grpc-netty:$grpcVersion")
    implementation("com.google.protobuf:protobuf-java:$protobufVersion")
    implementation("com.google.protobuf:protobuf-java-util:$protobufVersion")

    // Test
    testImplementation("org.junit.jupiter:junit-jupiter:5.12.1")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:$coroutinesVersion")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

kotlin {
    jvmToolchain(11)
}

tasks.test {
    useJUnitPlatform()
}
