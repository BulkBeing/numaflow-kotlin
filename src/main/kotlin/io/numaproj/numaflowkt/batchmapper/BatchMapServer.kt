package io.numaproj.numaflowkt.batchmapper

import io.numaproj.numaflowkt.batchmapper.internal.BatchMapperAdapter
import io.numaproj.numaflow.batchmapper.GRPCConfig
import io.numaproj.numaflow.batchmapper.Server as JavaServer

/**
 * Create and configure a Numaflow batch map server.
 *
 * This is the primary entry point for the Numaflow Kotlin BatchMapper SDK. The
 * returned [BatchMapServer] wraps the Java SDK's gRPC server and delegates all
 * batch map processing to the user-provided [BatchMapper] implementation.
 *
 * Example:
 * ```kotlin
 * fun main() {
 *     batchMapServer {
 *         batchMapper { messages ->
 *             messages.map { datum ->
 *                 BatchResponse(
 *                     id = datum.id,
 *                     messages = listOf(Message(
 *                         value = String(datum.value).uppercase().toByteArray(),
 *                         keys = datum.keys
 *                     ))
 *                 )
 *             }.toList()
 *         }
 *     }.run()
 * }
 * ```
 */
fun batchMapServer(block: BatchMapServerBuilder.() -> Unit): BatchMapServer {
    return BatchMapServerBuilder().apply(block).build()
}

/**
 * Marks DSL scope boundaries so that nested lambdas cannot accidentally
 * call functions from an outer receiver.
 */
@DslMarker
annotation class BatchMapServerDsl

/**
 * Builder for configuring a [BatchMapServer].
 *
 * Used inside the [batchMapServer] DSL block. Must have a [batchMapper] set before
 * [build] is called, or an [IllegalStateException] is thrown.
 */
@BatchMapServerDsl
class BatchMapServerBuilder internal constructor() {
    private var batchMapper: BatchMapper? = null
    private var config: BatchMapperConfig? = null

    /** Set the [BatchMapper] implementation. Required. */
    fun batchMapper(batchMapper: BatchMapper) { this.batchMapper = batchMapper }

    /**
     * Configure the gRPC server. If not called, the Java SDK's
     * `GRPCConfig.defaultGrpcConfig()` is used.
     */
    fun config(block: BatchMapperConfigBuilder.() -> Unit) {
        config = BatchMapperConfigBuilder().apply(block).build()
    }

    internal fun build(): BatchMapServer {
        return BatchMapServer(
            batchMapper = requireNotNull(batchMapper) { "batchMapper must be set" },
            config = config
        )
    }
}

/**
 * Builder for [BatchMapperConfig], used inside the `config { }` block of [batchMapServer].
 */
@BatchMapServerDsl
class BatchMapperConfigBuilder internal constructor() {
    var socketPath: String = BatchMapperConfig.DEFAULT_SOCKET_PATH
    var maxMessageSize: Int = BatchMapperConfig.DEFAULT_MAX_MESSAGE_SIZE
    var infoFilePath: String = BatchMapperConfig.DEFAULT_INFO_FILE_PATH
    var port: Int = BatchMapperConfig.DEFAULT_PORT
    var isLocal: Boolean = System.getenv(BatchMapperConfig.ENV_NUMAFLOW_POD) == null

    internal fun build(): BatchMapperConfig = BatchMapperConfig(
        socketPath = socketPath,
        maxMessageSize = maxMessageSize,
        infoFilePath = infoFilePath,
        port = port,
        isLocal = isLocal
    )
}

/**
 * A Numaflow batch map gRPC server.
 *
 * Call [run] to start the server and block until shutdown. The server shuts down
 * when the JVM receives a termination signal or when unrecoverable errors occur
 * in the user's [BatchMapper] implementation.
 *
 * Instances are created via [batchMapServer] and should not be constructed directly.
 */
class BatchMapServer internal constructor(
    private val batchMapper: BatchMapper,
    private val config: BatchMapperConfig?
) {
    /**
     * Starts the gRPC server and blocks until termination.
     *
     * This method does not return under normal operation. The server shuts down when:
     * - The JVM receives SIGTERM/SIGINT (Numaflow pod termination)
     * - An unrecoverable exception is thrown from [BatchMapper.processMessage]
     *
     * This is intentionally blocking (not `suspend`). The underlying Java SDK's
     * `server.start()` + `server.awaitTermination()` are fundamentally blocking JVM
     * calls that use JVM shutdown hooks, not coroutine cancellation.
     */
    fun run() {
        val adapter = BatchMapperAdapter(batchMapper)
        val server = if (config != null) {
            JavaServer(adapter, config.toJavaGRPCConfig())
        } else {
            JavaServer(adapter)
        }
        server.start()
        server.awaitTermination()
    }
}

/** Converts a [BatchMapperConfig] to the Java SDK's [GRPCConfig]. */
private fun BatchMapperConfig.toJavaGRPCConfig(): GRPCConfig =
    GRPCConfig.newBuilder()
        .socketPath(socketPath)
        .infoFilePath(infoFilePath)
        .maxMessageSize(maxMessageSize)
        .port(port)
        .isLocal(isLocal)
        .build()
