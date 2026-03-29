package io.numaproj.numaflowkt.mapper

import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.stub.StreamObserver
import io.numaproj.numaflow.map.v1.MapGrpc
import io.numaproj.numaflow.map.v1.MapOuterClass
import io.numaproj.numaflowkt.mapper.internal.MapperAdapter
import io.numaproj.numaflow.mapper.GRPCConfig
import io.numaproj.numaflow.mapper.Server as JavaServer
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.UUID
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Integration test kit for [Mapper] implementations.
 *
 * Starts a real gRPC server (not a mock) and provides a client that performs the full
 * Numaflow map protocol: handshake, request/response exchange, and cleanup. This
 * exercises the complete path including the adapter layer and gRPC serialization.
 *
 * **Thread safety:** Uses a [ConcurrentHashMap] for request-response correlation
 * because the gRPC response observer runs on a separate Netty thread, while
 * [sendRequest] blocks the test thread.
 *
 * **Port allocation:** Each test class should use a unique port to avoid conflicts
 * when tests run in parallel. Convention: mapper tests use ports 50071-50073.
 *
 * **No start delay needed:** The Java SDK's `Server.start()` is synchronous -- it
 * returns only when the gRPC server is bound and ready to accept connections.
 *
 * Implements [AutoCloseable] for use with `use { }` blocks in tests.
 *
 * Example:
 * ```kotlin
 * @Test
 * fun `test my mapper`() {
 *     MapperTestKit(myMapper, MapperConfig(port = 50071)).use { kit ->
 *         kit.start()
 *         val results = kit.sendRequest(
 *             keys = listOf("key1"),
 *             datum = Datum(value = "hello".toByteArray())
 *         )
 *         assertEquals(1, results.size)
 *     }
 * }
 * ```
 */
class MapperTestKit(
    private val mapper: Mapper,
    private val config: MapperConfig = MapperConfig()
) : AutoCloseable {

    private var server: JavaServer? = null
    private var channel: ManagedChannel? = null
    private var requestObserver: StreamObserver<MapOuterClass.MapRequest>? = null
    private val responseFutures = ConcurrentHashMap<String, CompletableFuture<MapOuterClass.MapResponse>>()

    /**
     * Start the gRPC server on `localhost:[config.port]` and perform the
     * initial protocol handshake (`Handshake(sot=true)`).
     *
     * Always forces `isLocal = true` regardless of the config, since tests
     * run on localhost over TCP (not Unix domain sockets).
     *
     * Must be called before [sendRequest].
     */
    fun start() {
        val adapter = MapperAdapter(mapper)
        val grpcConfig = GRPCConfig.newBuilder()
            .socketPath(config.socketPath)
            .infoFilePath(config.infoFilePath)
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
                val future = responseFutures[response.id]
                future?.complete(response)
            }
            override fun onError(t: Throwable) {
                responseFutures.values.forEach { it.completeExceptionally(t) }
            }
            override fun onCompleted() {
                responseFutures.values.removeIf { it.isDone }
            }
        })

        // Send handshake
        requestObserver!!.onNext(
            MapOuterClass.MapRequest.newBuilder()
                .setHandshake(MapOuterClass.Handshake.newBuilder().setSot(true))
                .build()
        )
    }

    /**
     * Send a single map request and return the output messages.
     *
     * Internally generates a unique request ID (UUID), registers a [CompletableFuture]
     * for that ID, sends the protobuf MapRequest, and blocks until the server responds
     * with a matching MapResponse or the [timeout] elapses.
     *
     * @param keys input message keys for routing/partitioning
     * @param datum input message containing the payload and optional metadata
     * @param timeout max wait time for the server response (default: 30 seconds)
     * @return list of output [Message]s from the mapper
     * @throws java.util.concurrent.TimeoutException if no response within [timeout]
     */
    fun sendRequest(
        keys: List<String> = emptyList(),
        datum: Datum,
        timeout: Duration = 30.seconds
    ): List<Message> {
        val reqObserver = requireNotNull(requestObserver) { "start() must be called first" }
        val requestId = UUID.randomUUID().toString()
        val future = CompletableFuture<MapOuterClass.MapResponse>()
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

        val response = future.get(timeout.inWholeMilliseconds, TimeUnit.MILLISECONDS)
        responseFutures.remove(requestId)

        return response.resultsList.map { result ->
            Message(
                value = result.value.toByteArray(),
                keys = result.keysList.toList(),
                tags = result.tagsList.toList()
            )
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
