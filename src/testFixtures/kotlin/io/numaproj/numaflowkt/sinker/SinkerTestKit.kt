package io.numaproj.numaflowkt.sinker

import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.stub.StreamObserver
import io.numaproj.numaflow.sink.v1.SinkGrpc
import io.numaproj.numaflow.sink.v1.SinkOuterClass
import io.numaproj.numaflowkt.sinker.internal.SinkerAdapter
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import io.numaproj.numaflow.sinker.GRPCConfig
import io.numaproj.numaflow.sinker.Server as JavaServer
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Integration test kit for [Sinker] implementations.
 *
 * Starts a real gRPC server backed by the Java SDK and provides a client that
 * performs the full Numaflow sink protocol (handshake -> data -> EOT -> response
 * collection). Use this to verify the complete path from serialization through
 * gRPC transport to user code and back.
 *
 * Published as a `testFixtures` source set — add to your project with:
 * ```kotlin
 * testImplementation(testFixtures("io.numaproj.numaflowkt:numaflow-kt:x.y.z"))
 * ```
 *
 * For simple unit tests that don't need gRPC, call [Sinker.processMessages]
 * directly with [kotlinx.coroutines.flow.flowOf].
 *
 * **Implementation note:** This TestKit is coupled to the protobuf schema
 * bundled with the pinned numaflow-java version. Schema changes in a future
 * Java SDK bump will require TestKit updates (compile-time errors, not runtime).
 *
 * Example:
 * ```kotlin
 * @Test
 * fun `integration test with real gRPC`() {
 *     SinkerTestKit(mySink).use { kit ->
 *         kit.start()
 *         val results = kit.sendMessages(
 *             Datum(id = "1", value = "hello".toByteArray()),
 *             Datum(id = "2", value = "world".toByteArray())
 *         )
 *         assertEquals(Response.Ok("1"), results[0])
 *         assertEquals(Response.Ok("2"), results[1])
 *     }
 * }
 * ```
 *
 * @param sinker the Kotlin [Sinker] implementation to test
 * @param config optional server configuration (defaults to [SinkerConfig] defaults)
 */
class SinkerTestKit(
    private val sinker: Sinker,
    private val config: SinkerConfig = SinkerConfig()
) : AutoCloseable {

    private var server: JavaServer? = null
    private var channel: ManagedChannel? = null

    /**
     * Start the gRPC server on `localhost:[config.port]`.
     * Must be called before [sendMessages].
     */
    fun start() {
        val adapter = SinkerAdapter(sinker)
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
    }

    /**
     * Send a batch of messages through the full gRPC sink protocol
     * (handshake -> data messages -> EOT) and return the [Response] list.
     *
     * Implements the Numaflow sink bidirectional streaming protocol:
     * 1. Client sends a [SinkOuterClass.Handshake] with `sot=true`
     * 2. Server acknowledges with the same handshake
     * 3. Client sends [SinkOuterClass.SinkRequest] for each datum
     * 4. Client sends [SinkOuterClass.TransmissionStatus] with `eot=true`
     * 5. Server responds with results followed by its own EOT
     * 6. Client calls `onCompleted()`
     *
     * @param datums the messages to send
     * @param timeout maximum time to wait for responses. Default: 30 seconds.
     * @return one [Response] per input datum
     * @throws IllegalStateException if [start] has not been called
     */
    fun sendMessages(vararg datums: Datum, timeout: Duration = 30.seconds): List<Response> {
        val ch = requireNotNull(channel) { "start() must be called before sendMessages()" }
        val stub = SinkGrpc.newStub(ch)

        val collected = mutableListOf<SinkOuterClass.SinkResponse>()
        val future = CompletableFuture<List<SinkOuterClass.SinkResponse>>()

        val responseObserver = object : StreamObserver<SinkOuterClass.SinkResponse> {
            override fun onNext(value: SinkOuterClass.SinkResponse) {
                collected.add(value)
            }

            override fun onError(t: Throwable) {
                future.completeExceptionally(t)
            }

            override fun onCompleted() {
                future.complete(collected.toList())
            }
        }

        val requestObserver = stub.sinkFn(responseObserver)

        // 1. Send handshake
        requestObserver.onNext(
            SinkOuterClass.SinkRequest.newBuilder()
                .setHandshake(SinkOuterClass.Handshake.newBuilder().setSot(true))
                .build()
        )

        // 2. Send data messages
        for (datum in datums) {
            val requestBuilder = SinkOuterClass.SinkRequest.Request.newBuilder()
                .setId(datum.id)
                .setValue(ByteString.copyFrom(datum.value))

            if (datum.keys.isNotEmpty()) requestBuilder.addAllKeys(datum.keys)
            datum.eventTime?.let {
                requestBuilder.setEventTime(
                    Timestamp.newBuilder()
                        .setSeconds(it.epochSecond)
                        .setNanos(it.nano)
                )
            }
            datum.watermark?.let {
                requestBuilder.setWatermark(
                    Timestamp.newBuilder()
                        .setSeconds(it.epochSecond)
                        .setNanos(it.nano)
                )
            }
            if (datum.headers.isNotEmpty()) requestBuilder.putAllHeaders(datum.headers)

            requestObserver.onNext(
                SinkOuterClass.SinkRequest.newBuilder()
                    .setRequest(requestBuilder)
                    .build()
            )
        }

        // 3. Send EOT
        requestObserver.onNext(
            SinkOuterClass.SinkRequest.newBuilder()
                .setStatus(SinkOuterClass.TransmissionStatus.newBuilder().setEot(true))
                .build()
        )

        // 4. Signal stream completion
        requestObserver.onCompleted()

        // 5. Collect and convert responses
        val timeoutMs = timeout.inWholeMilliseconds
        val responses = future.get(timeoutMs, TimeUnit.MILLISECONDS)
        return responses
            .filter { !it.handshake.sot }           // skip handshake ack
            .filter { !(it.hasStatus() && it.status.eot) } // skip EOT ack
            .flatMap { it.resultsList }
            .map { it.toKotlinResponse() }
    }

    /**
     * Stop the gRPC server and release resources.
     */
    override fun close() {
        channel?.shutdown()?.awaitTermination(5, TimeUnit.SECONDS)
        channel = null
        server?.stop()
        server = null
    }
}

/**
 * Converts a protobuf [SinkOuterClass.SinkResponse.Result] back to a Kotlin [Response].
 *
 * This is the reverse of the server-side conversion — used only in the TestKit
 * to verify round-trip correctness.
 */
private fun SinkOuterClass.SinkResponse.Result.toKotlinResponse(): Response = when (status) {
    SinkOuterClass.Status.SUCCESS -> Response.Ok(id)
    SinkOuterClass.Status.FAILURE -> Response.Failure(id, errMsg)
    SinkOuterClass.Status.FALLBACK -> Response.Fallback(id)
    SinkOuterClass.Status.SERVE -> Response.Serve(id, serveResponse.toByteArray())
    SinkOuterClass.Status.ON_SUCCESS -> {
        val msg = if (hasOnSuccessMsg() && onSuccessMsg != SinkOuterClass.SinkResponse.Result.Message.getDefaultInstance()) {
            Message(
                value = onSuccessMsg.value.toByteArray(),
                key = onSuccessMsg.keysList.firstOrNull() ?: "",
            )
        } else {
            Message(value = byteArrayOf())
        }
        Response.OnSuccess(id, msg)
    }
    else -> Response.Failure(id, "unknown status: $status")
}
