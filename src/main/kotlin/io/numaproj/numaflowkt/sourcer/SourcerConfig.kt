package io.numaproj.numaflowkt.sourcer

/**
 * Configuration for the sourcer gRPC server.
 *
 * All fields have sensible defaults matching the Java SDK. In production (inside a
 * Numaflow pod), the defaults work out of the box. For local development and testing,
 * override [port] and/or set [isLocal] = `true` to use TCP instead of Unix domain sockets.
 */
data class SourcerConfig(
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
        const val DEFAULT_SOCKET_PATH: String = "/var/run/numaflow/source.sock"
        const val DEFAULT_INFO_FILE_PATH: String = "/var/run/numaflow/sourcer-server-info"
    }
}
