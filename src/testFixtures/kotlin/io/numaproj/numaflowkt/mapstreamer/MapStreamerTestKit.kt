package io.numaproj.numaflowkt.mapstreamer

import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.stub.StreamObserver
import io.numaproj.numaflow.map.v1.MapGrpc
import io.numaproj.numaflow.map.v1.MapOuterClass
import io.numaproj.numaflowkt.mapstreamer.internal.MapStreamerAdapter
import io.numaproj.numaflow.mapstreamer.GRPCConfig
import io.numaproj.numaflow.mapstreamer.Server as JavaServer
import java.io.File
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.UUID
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Integration test kit for [MapStreamer] implementations.
 *
 * More complex than the Mapper TestKit because a MapStreamer produces multiple
 * responses per request, streamed back individually and terminated by an
 * end-of-transmission (EOT) marker. The TestKit accumulates all responses
 * in a [pendingResponses] buffer until the EOT arrives, then returns the
 * complete set.
 *
 * **EOT detection:** Matches by **both ID and EOT flag**
 * (`response.id == requestId && response.status.eot`). The Java SDK sends
 * per-request EOTs with the request ID attached, ensuring correct correlation
 * even if multiple requests are in-flight.
 *
 * **Thread safety:** Uses [ConcurrentHashMap] for both `responseFutures` and
 * `pendingResponses` because the gRPC response observer runs on a separate
 * Netty thread.
 *
 * **Port allocation:** MapStreamer tests use ports 50081-50083.
 *
 * Implements [AutoCloseable] for use with `use { }` blocks in tests.
 *
 * Example:
 * ```kotlin
 * MapStreamerTestKit(myStreamer, MapStreamerConfig(port = 50081)).use { kit ->
 *     kit.start()
 *     val results = kit.sendRequest(
 *         keys = listOf("key1"),
 *         datum = Datum(value = "hello world".toByteArray())
 *     )
 *     // results contains all streamed messages for this request
 * }
 * ```
 */
class MapStreamerTestKit(
    private val mapStreamer: MapStreamer,
    private val config: MapStreamerConfig = MapStreamerConfig()
) : AutoCloseable {

    private var server: JavaServer? = null
    private var channel: ManagedChannel? = null
    private var requestObserver: StreamObserver<MapOuterClass.MapRequest>? = null
    private val responseFutures = ConcurrentHashMap<String, CompletableFuture<List<MapOuterClass.MapResponse>>>()
    private val pendingResponses = ConcurrentHashMap<String, MutableList<MapOuterClass.MapResponse>>()

    /**
     * Start the gRPC server on `localhost:[config.port]` and perform the
     * initial protocol handshake (`Handshake(sot=true)`).
     *
     * Creates a temporary info file for the server (cleaned up on JVM exit)
     * and always forces `isLocal = true` for TCP-based testing.
     *
     * Must be called before [sendRequest].
     */
    fun start() {
        val adapter = MapStreamerAdapter(mapStreamer)
        val tempInfoFile = File.createTempFile("mapstreamer-server-info", ".json")
        tempInfoFile.deleteOnExit()
        val grpcConfig = GRPCConfig.newBuilder()
            .socketPath(config.socketPath)
            .infoFilePath(tempInfoFile.absolutePath)
            .maxMessageSize(config.maxMessageSize)
            .port(config.port)
            .isLocal(true)
            .build()
        val srv = JavaServer(adapter, grpcConfig)
        srv.start()
        server = srv

        channel = ManagedChannelBuilder
            .forAddress("localhost", config.port)
            .usePlaintext()
            .build()

        val stub = MapGrpc.newStub(channel)
        requestObserver = stub.mapFn(object : StreamObserver<MapOuterClass.MapResponse> {
            override fun onNext(response: MapOuterClass.MapResponse) {
                if (response.hasHandshake()) return

                val id = response.id
                // EOT response marks end of streaming for this request (match by ID + EOT)
                if (response.hasStatus() && response.status.eot) {
                    val pending = pendingResponses.remove(id)
                    responseFutures[id]?.complete(pending ?: emptyList())
                    return
                }

                pendingResponses.getOrPut(id) { mutableListOf() }.add(response)
            }
            override fun onError(t: Throwable) {
                responseFutures.values.forEach { it.completeExceptionally(t) }
            }
            override fun onCompleted() {}
        })

        // Send handshake
        requestObserver!!.onNext(
            MapOuterClass.MapRequest.newBuilder()
                .setHandshake(MapOuterClass.Handshake.newBuilder().setSot(true))
                .build()
        )
    }

    /**
     * Send a single request and collect all streamed output messages.
     *
     * The MapStreamer may emit multiple messages per request. This method
     * registers a [CompletableFuture], sends the protobuf MapRequest, and
     * blocks until the server sends the EOT marker for this request ID
     * or the [timeout] elapses. All intermediate responses are accumulated
     * in [pendingResponses] and returned as a flat list.
     *
     * @param keys input message keys for routing/partitioning
     * @param datum input message containing the payload and optional metadata
     * @param timeout max wait time for all streamed responses (default: 30 seconds)
     * @return list of all output [Message]s streamed for this request
     * @throws java.util.concurrent.TimeoutException if EOT not received within [timeout]
     */
    fun sendRequest(
        keys: List<String> = emptyList(),
        datum: Datum,
        timeout: Duration = 30.seconds
    ): List<Message> {
        val reqObserver = requireNotNull(requestObserver) { "start() must be called first" }
        val requestId = UUID.randomUUID().toString()
        val future = CompletableFuture<List<MapOuterClass.MapResponse>>()
        responseFutures[requestId] = future

        val requestBuilder = MapOuterClass.MapRequest.Request.newBuilder()
            .addAllKeys(keys)
            .setValue(ByteString.copyFrom(datum.value))

        datum.eventTime?.let {
            requestBuilder.setEventTime(
                Timestamp.newBuilder().setSeconds(it.epochSecond).setNanos(it.nano)
            )
        }
        datum.watermark?.let {
            requestBuilder.setWatermark(
                Timestamp.newBuilder().setSeconds(it.epochSecond).setNanos(it.nano)
            )
        }
        if (datum.headers.isNotEmpty()) requestBuilder.putAllHeaders(datum.headers)

        reqObserver.onNext(
            MapOuterClass.MapRequest.newBuilder()
                .setId(requestId)
                .setRequest(requestBuilder)
                .build()
        )

        val responses = future.get(timeout.inWholeMilliseconds, TimeUnit.MILLISECONDS)
        responseFutures.remove(requestId)

        return responses.flatMap { response ->
            response.resultsList.map { result ->
                Message(
                    value = result.value.toByteArray(),
                    keys = result.keysList.toList(),
                    tags = result.tagsList.toList()
                )
            }
        }
    }

    override fun close() {
        requestObserver?.onCompleted()
        channel?.shutdown()?.awaitTermination(5, TimeUnit.SECONDS)
        channel = null
        server?.stop()
        server = null
    }
}
