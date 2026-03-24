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
 *
 * All fields default to the same values as [SinkerConfig], which auto-resolve
 * socket paths and local mode from the `NUMAFLOW_UD_CONTAINER_TYPE` environment variable.
 */
@SinkServerDsl
class SinkerConfigBuilder {
    var socketPath: String = SinkerConfig.DEFAULT_SOCKET_PATH
    var maxMessageSize: Int = SinkerConfig.DEFAULT_MAX_MESSAGE_SIZE
    var infoFilePath: String = SinkerConfig.DEFAULT_INFO_FILE_PATH
    var port: Int = SinkerConfig.DEFAULT_PORT
    var isLocal: Boolean = System.getenv(SinkerConfig.ENV_UD_CONTAINER_TYPE) == null

    internal fun build(): SinkerConfig = SinkerConfig(
        socketPath = socketPath,
        maxMessageSize = maxMessageSize,
        infoFilePath = infoFilePath,
        port = port,
        isLocal = isLocal
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
     * This is intentionally blocking (not `suspend`). The underlying Java SDK's
     * `server.start()` + `server.awaitTermination()` are fundamentally blocking JVM
     * calls that use JVM shutdown hooks, not coroutine cancellation.
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
 */
private fun SinkerConfig.toJavaGRPCConfig(): GRPCConfig =
    GRPCConfig.newBuilder()
        .socketPath(socketPath)
        .infoFilePath(infoFilePath)
        .maxMessageSize(maxMessageSize)
        .port(port)
        .isLocal(isLocal)
        .build()
