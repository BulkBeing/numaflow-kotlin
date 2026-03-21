package io.numaproj.numaflowkt.sinker

import io.numaproj.numaflowkt.sinker.internal.SinkerAdapter
import io.numaproj.numaflow.sinker.GRPCConfig
import io.numaproj.numaflow.sinker.Server as JavaServer

/**
 * Creates a sink server with the given configuration.
 *
 * This is the primary entry point for the Numaflow Kotlin Sink SDK.
 *
 * Example — minimal:
 * ```kotlin
 * fun main() {
 *     sinkServer {
 *         sinker(mySink)
 *     }.run()
 * }
 * ```
 *
 * Example — with config:
 * ```kotlin
 * fun main() {
 *     sinkServer {
 *         sinker(mySink)
 *         config {
 *             maxMessageSize = 128 * 1024 * 1024
 *             port = 9090
 *         }
 *     }.run()
 * }
 * ```
 */
fun sinkServer(block: SinkServerBuilder.() -> Unit): SinkServer {
    val builder = SinkServerBuilder().apply(block)
    return builder.build()
}

/**
 * Marks DSL scope boundaries so that nested lambdas cannot accidentally
 * call functions from an outer receiver (e.g. calling `sinker()` inside `config {}`).
 */
@DslMarker
annotation class SinkServerDsl

/**
 * Builder for configuring a [SinkServer].
 *
 * Used inside the [sinkServer] DSL block. Must have a [sinker] set before
 * [build] is called, or an [IllegalStateException] is thrown.
 */
@SinkServerDsl
class SinkServerBuilder {
    private var sinker: Sinker? = null
    private var config: SinkerConfig? = null

    /** Set the [Sinker] implementation that will process messages. */
    fun sinker(sinker: Sinker) {
        this.sinker = sinker
    }

    /**
     * Configure the gRPC server. If not called, the Java SDK's
     * `GRPCConfig.defaultGrpcConfig()` is used, which auto-detects
     * socket paths and local-vs-production mode from environment variables.
     */
    fun config(block: SinkerConfigBuilder.() -> Unit) {
        config = SinkerConfigBuilder().apply(block).build()
    }

    internal fun build(): SinkServer {
        val sinker = requireNotNull(sinker) { "sinker must be provided" }
        return SinkServer(sinker, config)
    }
}

/**
 * Builder for [SinkerConfig], used inside the `config { }` block of [sinkServer].
 */
@SinkServerDsl
class SinkerConfigBuilder {
    var maxMessageSize: Int = SinkerConfig.DEFAULT_MAX_MESSAGE_SIZE
    var port: Int = SinkerConfig.DEFAULT_PORT

    internal fun build(): SinkerConfig = SinkerConfig(
        maxMessageSize = maxMessageSize,
        port = port
    )
}

/**
 * A Numaflow sink gRPC server.
 *
 * Call [run] to start the server and block until shutdown. The server shuts down
 * when the JVM receives a termination signal or when unrecoverable errors occur
 * in the user's [Sinker] implementation.
 *
 * Instances are created via [sinkServer] and should not be constructed directly.
 */
class SinkServer internal constructor(
    private val sinker: Sinker,
    private val config: SinkerConfig?
) {
    /**
     * Starts the gRPC server and blocks until termination.
     *
     * This method does not return under normal operation. The server shuts down when:
     * - The JVM receives SIGTERM/SIGINT (Numaflow pod termination)
     * - An unrecoverable exception is thrown from [Sinker.processMessages]
     *
     * Internally, this wraps the Kotlin [Sinker] in a [SinkerAdapter] that bridges
     * to the Java SDK, then delegates to the Java SDK's [Server][io.numaproj.numaflow.sinker.Server]
     * for gRPC lifecycle management.
     */
    fun run() {
        val adapter = SinkerAdapter(sinker)
        val server = if (config != null) {
            JavaServer(adapter, config.toJavaGRPCConfig())
            } else {
            // Let Java SDK auto-detect everything from environment variables
            JavaServer(adapter)
        }
        server.start()
        server.awaitTermination()
    }
}

/**
 * Converts a [SinkerConfig] to the Java SDK's [GRPCConfig].
 *
 * Replicates the environment-variable detection logic from the Java SDK's
 * package-private `GRPCConfig.defaultGrpcConfig()` method. The `NUMAFLOW_UD_CONTAINER_TYPE`
 * env var determines:
 * - Socket path (primary, fallback, or on-success sink)
 * - Server info file path
 * - Whether the server runs in local (TCP) or production (UDS) mode
 */
private fun SinkerConfig.toJavaGRPCConfig(): GRPCConfig {
    val containerType = System.getenv("NUMAFLOW_UD_CONTAINER_TYPE")

    val socketPath: String
    val infoFilePath: String
    when (containerType) {
        "fb-udsink" -> {
            socketPath = "/var/run/numaflow/fb-sink.sock"
            infoFilePath = "/var/run/numaflow/fb-sinker-server-info"
        }
        "ons-udsink" -> {
            socketPath = "/var/run/numaflow/ons-sink.sock"
            infoFilePath = "/var/run/numaflow/ons-sinker-server-info"
        }
        else -> {
            socketPath = "/var/run/numaflow/sink.sock"
            infoFilePath = "/var/run/numaflow/sinker-server-info"
        }
    }

    return GRPCConfig.newBuilder()
        .socketPath(socketPath)
        .infoFilePath(infoFilePath)
        .maxMessageSize(maxMessageSize)
        .port(port)
        .isLocal(containerType == null)
        .build()
}
