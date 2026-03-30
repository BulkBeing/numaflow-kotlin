package io.numaproj.numaflowkt.sourcetransformer

import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.stub.StreamObserver
import io.numaproj.numaflow.sourcetransformer.v1.SourceTransformGrpc
import io.numaproj.numaflow.sourcetransformer.v1.Sourcetransformer as SourceTransformProto
import io.numaproj.numaflowkt.sourcetransformer.internal.SourceTransformerAdapter
import io.numaproj.numaflow.sourcetransformer.GRPCConfig
import io.numaproj.numaflow.sourcetransformer.Server as JavaServer
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.UUID
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Integration test kit for [SourceTransformer] implementations.
 *
 * Starts a real gRPC server (not a mock) and provides a client that performs the full
 * Numaflow source transform protocol: handshake, request/response exchange, and cleanup.
 * This exercises the complete path including the adapter layer and gRPC serialization.
 *
 * **Thread safety:** Uses a [ConcurrentHashMap] for request-response correlation
 * because the gRPC response observer runs on a separate Netty thread, while
 * [sendRequest] blocks the test thread.
 *
 * **Port allocation:** Source transformer tests should use ports 50101-50103
 * to avoid conflicts with other modules when tests run in parallel.
 * (mapper: 50071-50073, mapstreamer: 50081-50083, batchmapper: 50091-50093)
 *
 * Implements [AutoCloseable] for use with `use { }` blocks in tests.
 *
 * Example:
 * ```kotlin
 * @Test
 * fun `test my transformer`() {
 *     val now = Instant.now()
 *     SourceTransformerTestKit(myTransformer, SourceTransformerConfig(port = 50101)).use { kit ->
 *         kit.start()
 *         val results = kit.sendRequest(
 *             keys = listOf("key1"),
 *             datum = Datum(value = "hello".toByteArray(), eventTime = now, watermark = now)
 *         )
 *         assertEquals(1, results.size)
 *     }
 * }
 * ```
 */
class SourceTransformerTestKit(
    private val sourceTransformer: SourceTransformer,
    private val config: SourceTransformerConfig = SourceTransformerConfig()
) : AutoCloseable {

    private var server: JavaServer? = null
    private var channel: ManagedChannel? = null
    private var requestObserver: StreamObserver<SourceTransformProto.SourceTransformRequest>? = null
    private val responseFutures = ConcurrentHashMap<String, CompletableFuture<SourceTransformProto.SourceTransformResponse>>()

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
        val adapter = SourceTransformerAdapter(sourceTransformer)
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

        val stub = SourceTransformGrpc.newStub(channel)
        requestObserver = stub.sourceTransformFn(object : StreamObserver<SourceTransformProto.SourceTransformResponse> {
            override fun onNext(response: SourceTransformProto.SourceTransformResponse) {
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

        // Send handshake: the server expects the first message to be a handshake
        // with sot=true before any data requests can be sent.
        requestObserver!!.onNext(
            SourceTransformProto.SourceTransformRequest.newBuilder()
                .setHandshake(SourceTransformProto.Handshake.newBuilder().setSot(true))
                .build()
        )
    }

    /**
     * Send a single source transform request and return the output messages.
     *
     * Internally generates a unique request ID (UUID), registers a [CompletableFuture]
     * for that ID, sends the protobuf SourceTransformRequest, and blocks until the
     * server responds with a matching SourceTransformResponse or the [timeout] elapses.
     *
     * @param keys input message keys for routing/partitioning
     * @param datum input message containing the payload and metadata
     * @param timeout max wait time for the server response (default: 30 seconds)
     * @return list of output [Message]s from the transformer
     * @throws java.util.concurrent.TimeoutException if no response within [timeout]
     */
    fun sendRequest(
        keys: List<String> = emptyList(),
        datum: Datum,
        timeout: Duration = 30.seconds
    ): List<Message> {
        val reqObserver = requireNotNull(requestObserver) { "start() must be called first" }
        val requestId = UUID.randomUUID().toString()
        val future = CompletableFuture<SourceTransformProto.SourceTransformResponse>()
        responseFutures[requestId] = future

        // Build the inner Request message with payload, timestamps, and correlation ID.
        // The request ID is set on the inner Request (not the outer SourceTransformRequest).
        val requestBuilder = SourceTransformProto.SourceTransformRequest.Request.newBuilder()
            .addAllKeys(keys)
            .setValue(ByteString.copyFrom(datum.value))
            .setId(requestId)
            .setEventTime(
                Timestamp.newBuilder()
                    .setSeconds(datum.eventTime.epochSecond)
                    .setNanos(datum.eventTime.nano)
            )
            .setWatermark(
                Timestamp.newBuilder()
                    .setSeconds(datum.watermark.epochSecond)
                    .setNanos(datum.watermark.nano)
            )

        if (datum.headers.isNotEmpty()) requestBuilder.putAllHeaders(datum.headers)

        reqObserver.onNext(
            SourceTransformProto.SourceTransformRequest.newBuilder()
                .setRequest(requestBuilder)
                .build()
        )

        val response = future.get(timeout.inWholeMilliseconds, TimeUnit.MILLISECONDS)
        responseFutures.remove(requestId)

        // Convert protobuf results back to Kotlin Message objects.
        // The response ID is on the outer SourceTransformResponse, while
        // individual results carry event_time as a protobuf Timestamp.
        return response.resultsList.map { result ->
            Message(
                value = result.value.toByteArray(),
                eventTime = Instant.ofEpochSecond(
                    result.eventTime.seconds,
                    result.eventTime.nanos.toLong()
                ),
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
