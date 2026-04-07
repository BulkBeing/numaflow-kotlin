package io.numaproj.numaflowkt.sourcer

@DslMarker
annotation class SourceServerDsl

@SourceServerDsl
class SourceServerBuilder internal constructor() {
    private var sourcer: Sourcer? = null
    private var config: SourcerConfig? = null

    /** Set the [Sourcer] implementation. Required. */
    fun sourcer(sourcer: Sourcer) { this.sourcer = sourcer }

    /**
     * Configure the gRPC server. If not called, the Java SDK's
     * defaults are used (auto-detects local-vs-production mode).
     */
    fun config(block: SourcerConfigBuilder.() -> Unit) {
        config = SourcerConfigBuilder().apply(block).build()
    }

    internal fun build(): SourceServer {
        return SourceServer(
            sourcer = requireNotNull(sourcer) { "sourcer must be set" },
            config = config
        )
    }
}

@SourceServerDsl
class SourcerConfigBuilder internal constructor() {
    var socketPath: String = SourcerConfig.DEFAULT_SOCKET_PATH
    var maxMessageSize: Int = SourcerConfig.DEFAULT_MAX_MESSAGE_SIZE
    var infoFilePath: String = SourcerConfig.DEFAULT_INFO_FILE_PATH
    var port: Int = SourcerConfig.DEFAULT_PORT
    var isLocal: Boolean = System.getenv(SourcerConfig.ENV_NUMAFLOW_POD) == null

    internal fun build(): SourcerConfig = SourcerConfig(
        socketPath = socketPath,
        maxMessageSize = maxMessageSize,
        infoFilePath = infoFilePath,
        port = port,
        isLocal = isLocal
    )
}
