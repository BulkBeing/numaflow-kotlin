package io.numaproj.numaflowkt.sourcetransformer

import io.numaproj.numaflowkt.sourcetransformer.internal.SourceTransformerAdapter
import io.numaproj.numaflow.sourcetransformer.GRPCConfig
import io.numaproj.numaflow.sourcetransformer.Server as JavaServer

/**
 * Create and configure a Numaflow source transform server.
 *
 * This is the primary entry point for the Numaflow Kotlin Source Transform SDK.
 * The returned [SourceTransformerServer] wraps the Java SDK's gRPC server and
 * delegates all transform processing to the user-provided [SourceTransformer].
 *
 * Example -- minimal:
 * ```kotlin
 * fun main() {
 *     sourceTransformerServer {
 *         sourceTransformer { keys, datum ->
 *             listOf(Message(
 *                 value = datum.value,
 *                 eventTime = datum.eventTime,
 *                 keys = keys
 *             ))
 *         }
 *     }.run()
 * }
 * ```
 *
 * Example -- with config:
 * ```kotlin
 * fun main() {
 *     sourceTransformerServer {
 *         sourceTransformer { keys, datum ->
 *             listOf(Message(
 *                 value = datum.value,
 *                 eventTime = datum.eventTime,
 *                 keys = keys
 *             ))
 *         }
 *         config {
 *             maxMessageSize = 128 * 1024 * 1024
 *         }
 *     }.run()
 * }
 * ```
 */
fun sourceTransformerServer(block: SourceTransformerServerBuilder.() -> Unit): SourceTransformerServer {
    return SourceTransformerServerBuilder().apply(block).build()
}

/**
 * Marks DSL scope boundaries so that nested lambdas cannot accidentally
 * call functions from an outer receiver (e.g. calling `sourceTransformer()` inside `config {}`).
 */
@DslMarker
annotation class SourceTransformerServerDsl

/**
 * Builder for configuring a [SourceTransformerServer].
 *
 * Used inside the [sourceTransformerServer] DSL block. Must have a [sourceTransformer]
 * set before [build] is called, or an [IllegalStateException] is thrown.
 */
@SourceTransformerServerDsl
class SourceTransformerServerBuilder internal constructor() {
    private var sourceTransformer: SourceTransformer? = null
    private var config: SourceTransformerConfig? = null

    /** Set the [SourceTransformer] implementation. Required. */
    fun sourceTransformer(sourceTransformer: SourceTransformer) {
        this.sourceTransformer = sourceTransformer
    }

    /**
     * Configure the gRPC server. If not called, the Java SDK's
     * `GRPCConfig.defaultGrpcConfig()` is used, which auto-detects
     * local-vs-production mode from environment variables.
     */
    fun config(block: SourceTransformerConfigBuilder.() -> Unit) {
        config = SourceTransformerConfigBuilder().apply(block).build()
    }

    internal fun build(): SourceTransformerServer {
        return SourceTransformerServer(
            sourceTransformer = requireNotNull(sourceTransformer) {
                "sourceTransformer must be set"
            },
            config = config
        )
    }
}

/**
 * Builder for [SourceTransformerConfig], used inside the `config { }` block
 * of [sourceTransformerServer].
 */
@SourceTransformerServerDsl
class SourceTransformerConfigBuilder internal constructor() {
    var socketPath: String = SourceTransformerConfig.DEFAULT_SOCKET_PATH
    var maxMessageSize: Int = SourceTransformerConfig.DEFAULT_MAX_MESSAGE_SIZE
    var infoFilePath: String = SourceTransformerConfig.DEFAULT_INFO_FILE_PATH
    var port: Int = SourceTransformerConfig.DEFAULT_PORT
    var isLocal: Boolean = System.getenv(SourceTransformerConfig.ENV_NUMAFLOW_POD) == null

    internal fun build(): SourceTransformerConfig = SourceTransformerConfig(
        socketPath = socketPath,
        maxMessageSize = maxMessageSize,
        infoFilePath = infoFilePath,
        port = port,
        isLocal = isLocal
    )
}

/**
 * A Numaflow source transform gRPC server.
 *
 * Call [run] to start the server and block until shutdown. The server shuts down
 * when the JVM receives a termination signal or when unrecoverable errors occur
 * in the user's [SourceTransformer] implementation.
 *
 * Instances are created via [sourceTransformerServer] and should not be constructed directly.
 */
class SourceTransformerServer internal constructor(
    private val sourceTransformer: SourceTransformer,
    private val config: SourceTransformerConfig?
) {
    /**
     * Starts the gRPC server and blocks until termination.
     *
     * This method does not return under normal operation. The server shuts down when:
     * - The JVM receives SIGTERM/SIGINT (Numaflow pod termination)
     * - An unrecoverable exception is thrown from [SourceTransformer.processMessage]
     *
     * This is intentionally blocking (not `suspend`). The underlying Java SDK's
     * `server.start()` + `server.awaitTermination()` are fundamentally blocking JVM
     * calls that use JVM shutdown hooks, not coroutine cancellation.
     */
    fun run() {
        val adapter = SourceTransformerAdapter(sourceTransformer)
        val server = if (config != null) {
            JavaServer(adapter, config.toJavaGRPCConfig())
        } else {
            JavaServer(adapter)
        }
        server.start()
        server.awaitTermination()
    }
}

/** Converts a [SourceTransformerConfig] to the Java SDK's [GRPCConfig]. */
private fun SourceTransformerConfig.toJavaGRPCConfig(): GRPCConfig =
    GRPCConfig.newBuilder()
        .socketPath(socketPath)
        .infoFilePath(infoFilePath)
        .maxMessageSize(maxMessageSize)
        .port(port)
        .isLocal(isLocal)
        .build()
