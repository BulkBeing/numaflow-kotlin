package io.numaproj.numaflowkt.sinker

/**
 * Configuration for the sink gRPC server.
 *
 * Mirrors the Java SDK's `GRPCConfig` with identical defaults. Socket and info
 * file paths are auto-resolved from the `NUMAFLOW_UD_CONTAINER_TYPE` environment
 * variable to support primary, fallback, and on-success sink types.
 *
 * Users typically don't need to customize these unless running in local
 * development mode or overriding the max message size.
 *
 * @property socketPath      UDS socket path. Auto-resolved for primary/fallback/on-success sinks.
 * @property maxMessageSize  Maximum gRPC message size in bytes. Default: 64 MB.
 * @property infoFilePath    Path for the server info JSON file. Auto-resolved per sink type.
 * @property port            TCP port for local development mode. Default: 50051.
 * @property isLocal         Whether to run in local (TCP) mode. Auto-detected: `true` when
 *                           `NUMAFLOW_UD_CONTAINER_TYPE` is not set.
 */
data class SinkerConfig(
    val socketPath: String = DEFAULT_SOCKET_PATH,
    val maxMessageSize: Int = DEFAULT_MAX_MESSAGE_SIZE,
    val infoFilePath: String = DEFAULT_INFO_FILE_PATH,
    val port: Int = DEFAULT_PORT,
    val isLocal: Boolean = System.getenv(ENV_UD_CONTAINER_TYPE) == null
) {
    companion object {
        internal const val ENV_UD_CONTAINER_TYPE = "NUMAFLOW_UD_CONTAINER_TYPE"
        private const val CONTAINER_FALLBACK_SINK = "fb-udsink"
        private const val CONTAINER_ON_SUCCESS_SINK = "ons-udsink"

        /** 64 MB — matches the Java SDK's default. */
        const val DEFAULT_MAX_MESSAGE_SIZE: Int = 64 * 1024 * 1024

        /** Default TCP port for local development mode. */
        const val DEFAULT_PORT: Int = 50051

        // Auto-resolved based on container type environment variable
        private val containerType: String? = System.getenv(ENV_UD_CONTAINER_TYPE)

        /** Default UDS socket path, resolved per sink type. */
        val DEFAULT_SOCKET_PATH: String = when (containerType) {
            CONTAINER_FALLBACK_SINK -> "/var/run/numaflow/fb-sink.sock"
            CONTAINER_ON_SUCCESS_SINK -> "/var/run/numaflow/ons-sink.sock"
            else -> "/var/run/numaflow/sink.sock"
        }

        /** Default server info file path, resolved per sink type. */
        val DEFAULT_INFO_FILE_PATH: String = when (containerType) {
            CONTAINER_FALLBACK_SINK -> "/var/run/numaflow/fb-sinker-server-info"
            CONTAINER_ON_SUCCESS_SINK -> "/var/run/numaflow/ons-sinker-server-info"
            else -> "/var/run/numaflow/sinker-server-info"
        }
    }
}
