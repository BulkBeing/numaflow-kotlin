package io.numaproj.numaflowkt.batchmapper

import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.stub.StreamObserver
import io.numaproj.numaflow.map.v1.MapGrpc
import io.numaproj.numaflow.map.v1.MapOuterClass
import io.numaproj.numaflowkt.batchmapper.internal.BatchMapperAdapter
import io.numaproj.numaflow.batchmapper.GRPCConfig
import io.numaproj.numaflow.batchmapper.Server as JavaServer
import java.io.File
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Integration test kit for [BatchMapper] implementations.
 *
 * Unlike the Mapper and MapStreamer TestKits which reuse a single gRPC stream,
 * this TestKit creates a **fresh gRPC stream per [sendBatch] call**, matching
 * the Java SDK's batch protocol where each batch is a complete stream lifecycle:
 * handshake -> data -> client EOT -> responses -> server EOT -> stream close.
 *
 * **Port allocation:** BatchMapper tests use ports 50091-50093.
 *
 * Implements [AutoCloseable] for use with `use { }` blocks in tests.
 *
 * Example:
 * ```kotlin
 * BatchMapperTestKit(myBatchMapper, BatchMapperConfig(port = 50091)).use { kit ->
 *     kit.start()
 *     val results = kit.sendBatch(
 *         Datum(id = "1", value = "hello".toByteArray()),
 *         Datum(id = "2", value = "world".toByteArray())
 *     )
 *     // results is Map<String, List<Message>> keyed by datum ID
 * }
 * ```
 */
class BatchMapperTestKit(
    private val batchMapper: BatchMapper,
    private val config: BatchMapperConfig = BatchMapperConfig()
) : AutoCloseable {

    private var server: JavaServer? = null
    private var channel: ManagedChannel? = null

    /**
     * Start the gRPC server on `localhost:[config.port]`.
     *
     * Creates a temporary info file for the server (cleaned up on JVM exit)
     * and always forces `isLocal = true` for TCP-based testing. Unlike Mapper
     * and MapStreamer TestKits, no handshake is sent here -- each [sendBatch]
     * call opens its own fresh stream with its own handshake.
     *
     * Must be called before [sendBatch].
     */
    fun start() {
        val adapter = BatchMapperAdapter(batchMapper)
        val tempInfoFile = File.createTempFile("batchmapper-server-info", ".json")
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
    }

    /**
     * Send a batch of messages and return the mapped results.
     *
     * Opens a **fresh bidirectional gRPC stream** for this batch (matching the
     * Java SDK's batch-per-stream protocol) and executes the full lifecycle:
     *
     * 1. **Handshake** -- sends `Handshake(sot=true)`, server acknowledges
     * 2. **Data** -- sends one `MapRequest` per datum (ID from [Datum.id])
     * 3. **Client EOT** -- sends `TransmissionStatus(eot=true)` to signal batch boundary
     * 4. **Collect** -- accumulates `MapResponse` messages until server sends its EOT
     *    (identified by empty ID + `status.eot`)
     * 5. **Close** -- signals stream completion
     *
     * @param datums the batch of input messages (vararg for convenience in tests)
     * @param timeout max wait time for all batch responses (default: 30 seconds)
     * @return map from datum ID to list of output [Message]s
     * @throws java.util.concurrent.TimeoutException if server EOT not received within [timeout]
     */
    fun sendBatch(
        vararg datums: Datum,
        timeout: Duration = 30.seconds
    ): Map<String, List<Message>> {
        val ch = requireNotNull(channel) { "start() must be called first" }
        val stub = MapGrpc.newStub(ch)

        val collected = mutableListOf<MapOuterClass.MapResponse>()
        val future = CompletableFuture<List<MapOuterClass.MapResponse>>()

        val responseObserver = object : StreamObserver<MapOuterClass.MapResponse> {
            override fun onNext(response: MapOuterClass.MapResponse) {
                if (response.hasHandshake()) return
                // Server EOT signals all batch responses have been sent
                if (response.id.isEmpty() && response.hasStatus() && response.status.eot) {
                    future.complete(collected.toList())
                    return
                }
                collected.add(response)
            }
            override fun onError(t: Throwable) { future.completeExceptionally(t) }
            override fun onCompleted() {
                if (!future.isDone) future.complete(collected.toList())
            }
        }

        val requestObserver = stub.mapFn(responseObserver)

        // 1. Handshake
        requestObserver.onNext(
            MapOuterClass.MapRequest.newBuilder()
                .setHandshake(MapOuterClass.Handshake.newBuilder().setSot(true))
                .build()
        )

        // 2. Send data
        for (datum in datums) {
            val requestBuilder = MapOuterClass.MapRequest.Request.newBuilder()
                .addAllKeys(datum.keys)
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

            requestObserver.onNext(
                MapOuterClass.MapRequest.newBuilder()
                    .setId(datum.id)
                    .setRequest(requestBuilder)
                    .build()
            )
        }

        // 3. Send client EOT
        requestObserver.onNext(
            MapOuterClass.MapRequest.newBuilder()
                .setStatus(MapOuterClass.TransmissionStatus.newBuilder().setEot(true))
                .build()
        )

        // 4. Collect responses
        val responses = future.get(timeout.inWholeMilliseconds, TimeUnit.MILLISECONDS)

        // 5. Signal completion
        requestObserver.onCompleted()

        // Group by datum ID -> list of Messages
        return responses
            .filter { it.id.isNotEmpty() }
            .associate { response ->
                response.id to response.resultsList.map { result ->
                    Message(
                        value = result.value.toByteArray(),
                        keys = result.keysList.toList(),
                        tags = result.tagsList.toList()
                    )
                }
            }
    }

    override fun close() {
        channel?.shutdown()?.awaitTermination(5, TimeUnit.SECONDS)
        channel = null
        server?.stop()
        server = null
    }
}
