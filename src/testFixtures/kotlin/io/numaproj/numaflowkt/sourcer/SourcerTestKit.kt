package io.numaproj.numaflowkt.sourcer

import com.google.protobuf.ByteString
import com.google.protobuf.Empty
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.stub.StreamObserver
import io.numaproj.numaflow.source.v1.SourceGrpc
import io.numaproj.numaflow.source.v1.SourceOuterClass
import io.numaproj.numaflow.sourcer.GRPCConfig
import io.numaproj.numaflow.sourcer.Server as JavaServer
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Integration test kit for [Sourcer] implementations.
 *
 * Starts a real gRPC server and provides methods to exercise the full source
 * protocol: read (with handshake + EOT), ack (with handshake), nack (unary),
 * pending (unary), and partitions (unary).
 *
 * Each method opens a fresh gRPC stream (for streaming RPCs) or makes a fresh
 * unary call. No persistent state is held between calls.
 *
 * Port allocation: sourcer tests should use ports 50111-50113 to avoid
 * conflicts with other modules when tests run in parallel.
 * (mapper: 50071-50073, mapstreamer: 50081-50083, batchmapper: 50091-50093,
 *  sourcetransformer: 50101-50103)
 *
 * Example:
 * ```kotlin
 * SourcerTestKit(mySourcer, SourcerConfig(port = 50111)).use { kit ->
 *     kit.start()
 *     val messages = kit.read(count = 5, readTimeout = 1.seconds)
 *     kit.ack(messages.map { it.offset })
 *     assertEquals(0, kit.getPending())
 * }
 * ```
 */
class SourcerTestKit(
    private val sourcer: Sourcer,
    private val config: SourcerConfig = SourcerConfig()
) : AutoCloseable {

    private var server: JavaServer? = null
    private var channel: ManagedChannel? = null

    fun start() {
        val adapter = SourcerAdapter(sourcer)
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
     * Send a read request and collect all returned messages (until EOT).
     *
     * Opens a fresh bidirectional stream, sends handshake + read request,
     * collects message responses until the EOT marker, then closes the stream.
     *
     * @param count      Maximum number of messages to read.
     * @param readTimeout How long the sourcer should wait for messages (sent as
     *                    proto `timeout_in_ms`). This is the sourcer-side timeout.
     * @param callTimeout How long this method waits for the gRPC call to complete.
     *                    This is the test-side timeout.
     */
    fun read(
        count: Long,
        readTimeout: Duration = 1.seconds,
        callTimeout: Duration = 30.seconds
    ): List<Message> {
        val ch = requireNotNull(channel) { "start() must be called first" }
        val stub = SourceGrpc.newStub(ch)
        val messages = mutableListOf<Message>()
        val future = CompletableFuture<List<Message>>()

        val requestObserver = stub.readFn(object : StreamObserver<SourceOuterClass.ReadResponse> {
            override fun onNext(response: SourceOuterClass.ReadResponse) {
                // Skip handshake echo
                if (response.hasHandshake()) return
                // EOT marker signals batch complete
                if (response.hasStatus() && response.status.eot) return
                // Collect data messages
                if (response.hasResult()) {
                    val result = response.result
                    messages.add(Message(
                        value = result.payload.toByteArray(),
                        offset = Offset(
                            value = result.offset.offset.toByteArray(),
                            partitionId = result.offset.partitionId
                        ),
                        eventTime = Instant.ofEpochSecond(
                            result.eventTime.seconds,
                            result.eventTime.nanos.toLong()
                        ),
                        keys = result.keysList.toList(),
                        headers = result.headersMap.toMap()
                    ))
                }
            }
            override fun onError(t: Throwable) { future.completeExceptionally(t) }
            override fun onCompleted() { future.complete(messages) }
        })

        // Send handshake (required for ReadFn bidi stream)
        requestObserver.onNext(
            SourceOuterClass.ReadRequest.newBuilder()
                .setHandshake(SourceOuterClass.Handshake.newBuilder().setSot(true))
                .build()
        )

        // Send read request
        requestObserver.onNext(
            SourceOuterClass.ReadRequest.newBuilder()
                .setRequest(SourceOuterClass.ReadRequest.Request.newBuilder()
                    .setNumRecords(count)
                    .setTimeoutInMs(readTimeout.inWholeMilliseconds.toInt()))
                .build()
        )

        // Signal we're done sending
        requestObserver.onCompleted()

        return future.get(callTimeout.inWholeMilliseconds, TimeUnit.MILLISECONDS)
    }

    /**
     * Acknowledge the given offsets.
     *
     * Opens a fresh bidirectional stream with handshake (required for AckFn).
     */
    fun ack(offsets: List<Offset>, callTimeout: Duration = 30.seconds) {
        val ch = requireNotNull(channel) { "start() must be called first" }
        val stub = SourceGrpc.newStub(ch)
        val future = CompletableFuture<Unit>()

        val requestObserver = stub.ackFn(object : StreamObserver<SourceOuterClass.AckResponse> {
            override fun onNext(response: SourceOuterClass.AckResponse) {
                // Skip handshake echo
                if (response.hasHandshake()) return
            }
            override fun onError(t: Throwable) { future.completeExceptionally(t) }
            override fun onCompleted() { future.complete(Unit) }
        })

        // Send handshake (required for AckFn bidi stream)
        requestObserver.onNext(
            SourceOuterClass.AckRequest.newBuilder()
                .setHandshake(SourceOuterClass.Handshake.newBuilder().setSot(true))
                .build()
        )

        // Send ack request
        val protoOffsets = offsets.map { offset ->
            SourceOuterClass.Offset.newBuilder()
                .setOffset(ByteString.copyFrom(offset.value))
                .setPartitionId(offset.partitionId)
                .build()
        }
        requestObserver.onNext(
            SourceOuterClass.AckRequest.newBuilder()
                .setRequest(SourceOuterClass.AckRequest.Request.newBuilder()
                    .addAllOffsets(protoOffsets))
                .build()
        )

        requestObserver.onCompleted()
        future.get(callTimeout.inWholeMilliseconds, TimeUnit.MILLISECONDS)
    }

    /**
     * Negatively acknowledge the given offsets.
     *
     * NackFn is a unary RPC -- no handshake needed.
     */
    fun nack(offsets: List<Offset>, callTimeout: Duration = 30.seconds) {
        val ch = requireNotNull(channel) { "start() must be called first" }
        val stub = SourceGrpc.newStub(ch)
        val future = CompletableFuture<Unit>()

        val protoOffsets = offsets.map { offset ->
            SourceOuterClass.Offset.newBuilder()
                .setOffset(ByteString.copyFrom(offset.value))
                .setPartitionId(offset.partitionId)
                .build()
        }

        stub.nackFn(
            SourceOuterClass.NackRequest.newBuilder()
                .setRequest(SourceOuterClass.NackRequest.Request.newBuilder()
                    .addAllOffsets(protoOffsets))
                .build(),
            object : StreamObserver<SourceOuterClass.NackResponse> {
                override fun onNext(response: SourceOuterClass.NackResponse) {}
                override fun onError(t: Throwable) { future.completeExceptionally(t) }
                override fun onCompleted() { future.complete(Unit) }
            }
        )

        future.get(callTimeout.inWholeMilliseconds, TimeUnit.MILLISECONDS)
    }

    /** Get the number of pending messages. */
    fun getPending(callTimeout: Duration = 30.seconds): Long {
        val ch = requireNotNull(channel) { "start() must be called first" }
        val stub = SourceGrpc.newStub(ch)
        val future = CompletableFuture<Long>()

        stub.pendingFn(
            Empty.newBuilder().build(),
            object : StreamObserver<SourceOuterClass.PendingResponse> {
                override fun onNext(response: SourceOuterClass.PendingResponse) {
                    future.complete(response.result.count)
                }
                override fun onError(t: Throwable) { future.completeExceptionally(t) }
                override fun onCompleted() {
                    if (!future.isDone) future.completeExceptionally(
                        RuntimeException("Server completed without a response")
                    )
                }
            }
        )

        return future.get(callTimeout.inWholeMilliseconds, TimeUnit.MILLISECONDS)
    }

    /** Get the partition list. */
    fun getPartitions(callTimeout: Duration = 30.seconds): List<Int> {
        val ch = requireNotNull(channel) { "start() must be called first" }
        val stub = SourceGrpc.newStub(ch)
        val future = CompletableFuture<List<Int>>()

        stub.partitionsFn(
            Empty.newBuilder().build(),
            object : StreamObserver<SourceOuterClass.PartitionsResponse> {
                override fun onNext(response: SourceOuterClass.PartitionsResponse) {
                    future.complete(response.result.partitionsList.toList())
                }
                override fun onError(t: Throwable) { future.completeExceptionally(t) }
                override fun onCompleted() {
                    if (!future.isDone) future.completeExceptionally(
                        RuntimeException("Server completed without a response")
                    )
                }
            }
        )

        return future.get(callTimeout.inWholeMilliseconds, TimeUnit.MILLISECONDS)
    }

    override fun close() {
        channel?.shutdown()?.awaitTermination(5, TimeUnit.SECONDS)
        channel = null
        server?.stop()
        server = null
    }
}
