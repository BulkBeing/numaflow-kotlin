package io.numaproj.numaflowkt.mapstreamer

/**
 * Configuration for the map streamer gRPC server.
 *
 * All fields have sensible defaults matching the Java SDK. In production (inside a
 * Numaflow pod), the defaults work out of the box. For local development and testing,
 * override [port] and/or set [isLocal] = `true` to use TCP instead of Unix domain sockets.
 *
 * **Local mode auto-detection:** When the `NUMAFLOW_POD` environment variable is absent
 * (i.e. not running inside a Numaflow pod), [isLocal] defaults to `true` and the server
 * listens on TCP `localhost:[port]`. When present, the server uses the Unix domain socket
 * at [socketPath].
 *
 * **Socket path:** Uses `mapstream.sock` (not `map.sock`), matching the Java SDK's
 * MapStreamer server. The [infoFilePath] is shared with the mapper (`mapper-server-info`).
 *
 * @property socketPath     Unix domain socket path. Default: `/var/run/numaflow/mapstream.sock`.
 *                          Only used when [isLocal] is `false`.
 * @property maxMessageSize Maximum gRPC message size in bytes. Default: 64 MB.
 * @property infoFilePath   Path where the server writes its info JSON file, used by the
 *                          Numaflow platform for service discovery.
 * @property port           TCP port for local development mode. Default: 50051.
 *                          Only used when [isLocal] is `true`.
 * @property isLocal        Whether to run in local (TCP) mode instead of UDS mode.
 *                          Auto-detected from the `NUMAFLOW_POD` environment variable.
 */
data class MapStreamerConfig(
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
        const val DEFAULT_SOCKET_PATH: String = "/var/run/numaflow/mapstream.sock"
        const val DEFAULT_INFO_FILE_PATH: String = "/var/run/numaflow/mapper-server-info"
    }
}
