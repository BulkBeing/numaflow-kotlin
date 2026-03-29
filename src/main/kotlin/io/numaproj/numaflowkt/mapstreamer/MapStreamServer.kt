package io.numaproj.numaflowkt.mapstreamer

import io.numaproj.numaflowkt.mapstreamer.internal.MapStreamerAdapter
import io.numaproj.numaflow.mapstreamer.GRPCConfig
import io.numaproj.numaflow.mapstreamer.Server as JavaServer

/**
 * Create and configure a Numaflow map stream server.
 *
 * This is the primary entry point for the Numaflow Kotlin MapStreamer SDK. The
 * returned [MapStreamServer] wraps the Java SDK's gRPC server and delegates all
 * streaming map processing to the user-provided [MapStreamer] implementation.
 *
 * Example:
 * ```kotlin
 * fun main() {
 *     mapStreamServer {
 *         mapStreamer { keys, datum ->
 *             flow {
 *                 String(datum.value).split(" ").forEach { word ->
 *                     emit(Message(value = word.toByteArray(), keys = keys))
 *                 }
 *             }
 *         }
 *     }.run()
 * }
 * ```
 */
fun mapStreamServer(block: MapStreamServerBuilder.() -> Unit): MapStreamServer {
    return MapStreamServerBuilder().apply(block).build()
}

/**
 * Marks DSL scope boundaries so that nested lambdas cannot accidentally
 * call functions from an outer receiver.
 */
@DslMarker
annotation class MapStreamServerDsl

/**
 * Builder for configuring a [MapStreamServer].
 *
 * Used inside the [mapStreamServer] DSL block. Must have a [mapStreamer] set before
 * [build] is called, or an [IllegalStateException] is thrown.
 */
@MapStreamServerDsl
class MapStreamServerBuilder internal constructor() {
    private var mapStreamer: MapStreamer? = null
    private var config: MapStreamerConfig? = null

    /** Set the [MapStreamer] implementation. Required. */
    fun mapStreamer(mapStreamer: MapStreamer) { this.mapStreamer = mapStreamer }

    /**
     * Configure the gRPC server. If not called, the Java SDK's
     * `GRPCConfig.defaultGrpcConfig()` is used.
     */
    fun config(block: MapStreamerConfigBuilder.() -> Unit) {
        config = MapStreamerConfigBuilder().apply(block).build()
    }

    internal fun build(): MapStreamServer {
        return MapStreamServer(
            mapStreamer = requireNotNull(mapStreamer) { "mapStreamer must be set" },
            config = config
        )
    }
}

/**
 * Builder for [MapStreamerConfig], used inside the `config { }` block of [mapStreamServer].
 */
@MapStreamServerDsl
class MapStreamerConfigBuilder internal constructor() {
    var socketPath: String = MapStreamerConfig.DEFAULT_SOCKET_PATH
    var maxMessageSize: Int = MapStreamerConfig.DEFAULT_MAX_MESSAGE_SIZE
    var infoFilePath: String = MapStreamerConfig.DEFAULT_INFO_FILE_PATH
    var port: Int = MapStreamerConfig.DEFAULT_PORT
    var isLocal: Boolean = System.getenv(MapStreamerConfig.ENV_NUMAFLOW_POD) == null

    internal fun build(): MapStreamerConfig = MapStreamerConfig(
        socketPath = socketPath,
        maxMessageSize = maxMessageSize,
        infoFilePath = infoFilePath,
        port = port,
        isLocal = isLocal
    )
}

/**
 * A Numaflow map stream gRPC server.
 *
 * Call [run] to start the server and block until shutdown. The server shuts down
 * when the JVM receives a termination signal or when unrecoverable errors occur
 * in the user's [MapStreamer] implementation.
 *
 * Instances are created via [mapStreamServer] and should not be constructed directly.
 */
class MapStreamServer internal constructor(
    private val mapStreamer: MapStreamer,
    private val config: MapStreamerConfig?
) {
    /**
     * Starts the gRPC server and blocks until termination.
     *
     * This method does not return under normal operation. The server shuts down when:
     * - The JVM receives SIGTERM/SIGINT (Numaflow pod termination)
     * - An unrecoverable exception is thrown from [MapStreamer.processMessage]
     *
     * This is intentionally blocking (not `suspend`). The underlying Java SDK's
     * `server.start()` + `server.awaitTermination()` are fundamentally blocking JVM
     * calls that use JVM shutdown hooks, not coroutine cancellation.
     */
    fun run() {
        val adapter = MapStreamerAdapter(mapStreamer)
        val server = if (config != null) {
            JavaServer(adapter, config.toJavaGRPCConfig())
        } else {
            JavaServer(adapter)
        }
        server.start()
        server.awaitTermination()
    }
}

/** Converts a [MapStreamerConfig] to the Java SDK's [GRPCConfig]. */
private fun MapStreamerConfig.toJavaGRPCConfig(): GRPCConfig =
    GRPCConfig.newBuilder()
        .socketPath(socketPath)
        .infoFilePath(infoFilePath)
        .maxMessageSize(maxMessageSize)
        .port(port)
        .isLocal(isLocal)
        .build()
