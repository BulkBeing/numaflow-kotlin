package io.numaproj.numaflowkt.mapper

import io.numaproj.numaflowkt.mapper.internal.MapperAdapter
import io.numaproj.numaflow.mapper.GRPCConfig
import io.numaproj.numaflow.mapper.Server as JavaServer

/**
 * Create and configure a Numaflow map server.
 *
 * This is the primary entry point for the Numaflow Kotlin Map SDK. The returned
 * [MapServer] wraps the Java SDK's gRPC server and delegates all map processing
 * to the user-provided [Mapper] implementation.
 *
 * Example -- minimal:
 * ```kotlin
 * fun main() {
 *     mapServer {
 *         mapper { keys, datum ->
 *             val upper = String(datum.value).uppercase()
 *             listOf(Message(value = upper.toByteArray(), keys = keys))
 *         }
 *     }.run()
 * }
 * ```
 *
 * Example -- with config:
 * ```kotlin
 * fun main() {
 *     mapServer {
 *         mapper { keys, datum ->
 *             listOf(Message(value = datum.value, keys = keys))
 *         }
 *         config {
 *             maxMessageSize = 128 * 1024 * 1024
 *             port = 9090
 *         }
 *     }.run()
 * }
 * ```
 */
fun mapServer(block: MapServerBuilder.() -> Unit): MapServer {
    return MapServerBuilder().apply(block).build()
}

/**
 * Marks DSL scope boundaries so that nested lambdas cannot accidentally
 * call functions from an outer receiver (e.g. calling `mapper()` inside `config {}`).
 */
@DslMarker
annotation class MapServerDsl

/**
 * Builder for configuring a [MapServer].
 *
 * Used inside the [mapServer] DSL block. Must have a [mapper] set before
 * [build] is called, or an [IllegalStateException] is thrown.
 */
@MapServerDsl
class MapServerBuilder internal constructor() {
    private var mapper: Mapper? = null
    private var config: MapperConfig? = null

    /** Set the [Mapper] implementation. Required. */
    fun mapper(mapper: Mapper) { this.mapper = mapper }

    /**
     * Configure the gRPC server. If not called, the Java SDK's
     * `GRPCConfig.defaultGrpcConfig()` is used, which auto-detects
     * local-vs-production mode from environment variables.
     */
    fun config(block: MapperConfigBuilder.() -> Unit) {
        config = MapperConfigBuilder().apply(block).build()
    }

    internal fun build(): MapServer {
        return MapServer(
            mapper = requireNotNull(mapper) { "mapper must be set" },
            config = config
        )
    }
}

/**
 * Builder for [MapperConfig], used inside the `config { }` block of [mapServer].
 */
@MapServerDsl
class MapperConfigBuilder internal constructor() {
    var socketPath: String = MapperConfig.DEFAULT_SOCKET_PATH
    var maxMessageSize: Int = MapperConfig.DEFAULT_MAX_MESSAGE_SIZE
    var infoFilePath: String = MapperConfig.DEFAULT_INFO_FILE_PATH
    var port: Int = MapperConfig.DEFAULT_PORT
    var isLocal: Boolean = System.getenv(MapperConfig.ENV_NUMAFLOW_POD) == null

    internal fun build(): MapperConfig = MapperConfig(
        socketPath = socketPath,
        maxMessageSize = maxMessageSize,
        infoFilePath = infoFilePath,
        port = port,
        isLocal = isLocal
    )
}

/**
 * A Numaflow map gRPC server.
 *
 * Call [run] to start the server and block until shutdown. The server shuts down
 * when the JVM receives a termination signal or when unrecoverable errors occur
 * in the user's [Mapper] implementation.
 *
 * Instances are created via [mapServer] and should not be constructed directly.
 */
class MapServer internal constructor(
    private val mapper: Mapper,
    private val config: MapperConfig?
) {
    /**
     * Starts the gRPC server and blocks until termination.
     *
     * This method does not return under normal operation. The server shuts down when:
     * - The JVM receives SIGTERM/SIGINT (Numaflow pod termination)
     * - An unrecoverable exception is thrown from [Mapper.processMessage]
     *
     * This is intentionally blocking (not `suspend`). The underlying Java SDK's
     * `server.start()` + `server.awaitTermination()` are fundamentally blocking JVM
     * calls that use JVM shutdown hooks, not coroutine cancellation.
     */
    fun run() {
        val adapter = MapperAdapter(mapper)
        val server = if (config != null) {
            JavaServer(adapter, config.toJavaGRPCConfig())
        } else {
            JavaServer(adapter)
        }
        server.start()
        server.awaitTermination()
    }
}

/** Converts a [MapperConfig] to the Java SDK's [GRPCConfig]. */
private fun MapperConfig.toJavaGRPCConfig(): GRPCConfig =
    GRPCConfig.newBuilder()
        .socketPath(socketPath)
        .infoFilePath(infoFilePath)
        .maxMessageSize(maxMessageSize)
        .port(port)
        .isLocal(isLocal)
        .build()
