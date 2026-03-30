package io.numaproj.numaflowkt.sourcetransformer

/**
 * Configuration for the source transformer gRPC server.
 *
 * All fields have sensible defaults matching the Java SDK's
 * `io.numaproj.numaflow.sourcetransformer.Constants`. In production (inside a
 * Numaflow pod), the server binds to a Unix domain socket at [socketPath].
 * In local/development mode (`NUMAFLOW_POD` env var not set), it binds to
 * `localhost:[port]` over TCP instead.
 *
 * @property socketPath     Unix domain socket path (production mode).
 * @property maxMessageSize Maximum gRPC message size in bytes.
 * @property infoFilePath   Path for the server info JSON file written at startup.
 * @property port           TCP port for local development mode.
 * @property isLocal        Whether to run in local (TCP) vs UDS mode.
 *                          Auto-detected from the `NUMAFLOW_POD` environment variable.
 */
data class SourceTransformerConfig(
    val socketPath: String = DEFAULT_SOCKET_PATH,
    val maxMessageSize: Int = DEFAULT_MAX_MESSAGE_SIZE,
    val infoFilePath: String = DEFAULT_INFO_FILE_PATH,
    val port: Int = DEFAULT_PORT,
    val isLocal: Boolean = System.getenv(ENV_NUMAFLOW_POD) == null
) {
    companion object {
        internal const val ENV_NUMAFLOW_POD = "NUMAFLOW_POD"
        const val DEFAULT_MAX_MESSAGE_SIZE: Int = 64 * 1024 * 1024
        const val DEFAULT_PORT: Int = 50051
        const val DEFAULT_SOCKET_PATH: String = "/var/run/numaflow/sourcetransform.sock"
        const val DEFAULT_INFO_FILE_PATH: String = "/var/run/numaflow/sourcetransformer-server-info"
    }
}
