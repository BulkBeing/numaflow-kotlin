plugins {
    kotlin("jvm") version "2.3.0"
    id("org.jetbrains.dokka") version "2.1.0"
    id("org.jetbrains.dokka-javadoc") version "2.1.0"
    `java-test-fixtures`
    `maven-publish`
    signing
}

group = "io.numaproj.numaflowkt"
version = System.getenv("RELEASE_VERSION").takeUnless { it.isNullOrBlank() } ?: "0.1.0-SNAPSHOT"
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

    // gRPC/protobuf — needed by the TestKit (testFixtures) for the gRPC client
    // These are also transitive via numaflow-java, but declared explicitly for testFixtures access
    implementation("io.grpc:grpc-stub:$grpcVersion")
    implementation("io.grpc:grpc-protobuf:$grpcVersion")
    implementation("io.grpc:grpc-netty:$grpcVersion")
    implementation("com.google.protobuf:protobuf-java:$protobufVersion")
    implementation("com.google.protobuf:protobuf-java-util:$protobufVersion")

    // TestKit dependencies (testFixtures source set)
    testFixturesImplementation("io.numaproj.numaflow:numaflow-java:0.11.0")
    testFixturesImplementation("io.grpc:grpc-kotlin-stub:$grpcKotlinVersion")
    testFixturesImplementation("io.grpc:grpc-stub:$grpcVersion")
    testFixturesImplementation("io.grpc:grpc-protobuf:$grpcVersion")
    testFixturesImplementation("io.grpc:grpc-netty:$grpcVersion")
    testFixturesImplementation("com.google.protobuf:protobuf-java:$protobufVersion")

    // Test
    testImplementation(testFixtures(project(":")))
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

java {
    withSourcesJar()
}

val dokkaJavadocJar by tasks.registering(Jar::class) {
    dependsOn(tasks.dokkaGeneratePublicationJavadoc)
    from(tasks.dokkaGeneratePublicationJavadoc.flatMap { it.outputDirectory })
    archiveClassifier.set("javadoc")
}

publishing {
    publications {
        create<MavenPublication>("mavenKotlin") {
            from(components["java"])
            artifact(dokkaJavadocJar)

            groupId = "io.github.bulkbeing"
            artifactId = "numaflow-kotlin"
            version = project.version.toString()

            pom {
                name.set("numaflow-kotlin")
                description.set("Kotlin SDK for Numaflow")
                url.set("https://github.com/BulkBeing/numaflow-kotlin")
                licenses {
                    license {
                        name.set("Apache-2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0")
                    }
                }
                developers {
                    developer {
                        id.set("BulkBeing")
                        name.set("Sreekanth")
                        email.set("prsreekanth920@gmail.com")
                    }
                }
                scm {
                    url.set("https://github.com/BulkBeing/numaflow-kotlin")
                    connection.set("scm:git:git://github.com/BulkBeing/numaflow-kotlin.git")
                    developerConnection.set("scm:git:ssh://github.com/BulkBeing/numaflow-kotlin.git")
                }
            }
        }
    }
    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/BulkBeing/numaflow-kotlin")
            credentials {
                username = System.getenv("GITHUB_ACTOR")
                password = System.getenv("GITHUB_TOKEN")
            }
        }
    }
}

signing {
    isRequired = System.getenv("GPG_PRIVATE_KEY") != null
    val signingKey = System.getenv("GPG_PRIVATE_KEY")
    val signingPassword = System.getenv("GPG_PASSPHRASE") ?: ""
    useInMemoryPgpKeys(signingKey, signingPassword)
    sign(publishing.publications["mavenKotlin"])
}

