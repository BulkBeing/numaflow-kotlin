package io.numaproj.numaflowkt.sourcer

import io.numaproj.numaflow.sourcer.GRPCConfig
import io.numaproj.numaflow.sourcer.Server as JavaServer

/**
 * Create and configure a Numaflow source server.
 *
 * Example -- minimal:
 * ```kotlin
 * fun main() {
 *     sourceServer {
 *         sourcer(MySource())
 *     }.run()
 * }
 * ```
 *
 * Example -- with config:
 * ```kotlin
 * fun main() {
 *     sourceServer {
 *         sourcer(MySource())
 *         config {
 *             maxMessageSize = 128 * 1024 * 1024
 *         }
 *     }.run()
 * }
 * ```
 */
fun sourceServer(block: SourceServerBuilder.() -> Unit): SourceServer {
    return SourceServerBuilder().apply(block).build()
}

/**
 * A Numaflow source gRPC server.
 *
 * Call [run] to start the server and block until shutdown.
 */
class SourceServer internal constructor(
    private val sourcer: Sourcer,
    private val config: SourcerConfig?
) {
    /**
     * Starts the gRPC server and blocks until termination.
     *
     * This is intentionally blocking (not `suspend`). The underlying Java SDK's
     * `server.start()` + `server.awaitTermination()` are fundamentally blocking JVM
     * calls that use JVM shutdown hooks, not coroutine cancellation.
     */
    fun run() {
        val adapter = SourcerAdapter(sourcer)
        val server = if (config != null) {
            JavaServer(adapter, config.toJavaGRPCConfig())
        } else {
            JavaServer(adapter)
        }
        server.start()
        server.awaitTermination()
    }
}

private fun SourcerConfig.toJavaGRPCConfig(): GRPCConfig =
    GRPCConfig.newBuilder()
        .socketPath(socketPath)
        .infoFilePath(infoFilePath)
        .maxMessageSize(maxMessageSize)
        .port(port)
        .isLocal(isLocal)
        .build()
