package io.numaproj.numaflowkt.sinker

/**
 * Configuration for the sink gRPC server.
 *
 * All fields have sensible defaults matching the Java SDK. Users typically
 * don't need to customize these unless running in local development mode.
 *
 * Internal fields like `socketPath`, `infoFilePath`, and `isLocal` are
 * auto-detected from environment variables by the Java SDK's
 * `GRPCConfig.defaultGrpcConfig()`. Sink type detection (primary / fallback /
 * on-success) is likewise invisible — handled automatically via the
 * `NUMAFLOW_UD_CONTAINER_TYPE` environment variable.
 *
 * @property maxMessageSize  Maximum gRPC message size in bytes. Default: 64 MB.
 * @property port            TCP port for local development mode. Default: 50051.
 */
data class SinkerConfig(
    val maxMessageSize: Int = DEFAULT_MAX_MESSAGE_SIZE,
    val port: Int = DEFAULT_PORT
) {
    companion object {
        /** 64 MB — matches the Java SDK's default. */
        const val DEFAULT_MAX_MESSAGE_SIZE: Int = 64 * 1024 * 1024

        /** Default TCP port for local development mode. */
        const val DEFAULT_PORT: Int = 50051
    }
}
