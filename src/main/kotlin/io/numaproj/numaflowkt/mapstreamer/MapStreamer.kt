package io.numaproj.numaflowkt.mapstreamer

import kotlinx.coroutines.flow.Flow

/**
 * A user-defined streaming map function that produces output messages
 * incrementally as a [Flow].
 *
 * Unlike [io.numaproj.numaflowkt.mapper.Mapper] which returns all results at once,
 * MapStreamer emits results one at a time as they become available. Each emitted
 * [Message] is forwarded to the Java SDK's `OutputObserver` immediately (eager
 * forwarding), preserving true streaming semantics and reducing downstream latency.
 *
 * Because this is a `fun interface`, simple streamers can be written as lambdas:
 *
 * Example -- stream words from a sentence:
 * ```kotlin
 * val wordSplitter = MapStreamer { keys, datum ->
 *     flow {
 *         String(datum.value).split(" ").forEach { word ->
 *             emit(Message(value = word.toByteArray(), keys = keys))
 *         }
 *     }
 * }
 * ```
 *
 * Example -- streaming with async operations:
 * ```kotlin
 * val enricher = MapStreamer { keys, datum ->
 *     flow {
 *         val base = String(datum.value)
 *         emit(Message(value = "processing: $base".toByteArray()))
 *         val enriched = fetchFromApi(base)  // suspend call
 *         emit(Message(value = enriched.toByteArray(), keys = keys))
 *     }
 * }
 * ```
 *
 * @see mapStreamServer for wiring a MapStreamer into a gRPC server
 * @see io.numaproj.numaflowkt.mapper.Mapper for non-streaming (collect-all) output
 * @see io.numaproj.numaflowkt.batchmapper.BatchMapper for batch processing
 */
fun interface MapStreamer {
    /**
     * Process a single message and return a [Flow] of output messages.
     *
     * This is a `suspend` function, so implementations can perform suspend setup
     * (e.g. acquiring a connection) before returning the Flow. The flow is collected
     * by the SDK -- each emitted [Message] is sent to the downstream immediately.
     * After the flow completes, the Java SDK's Akka actor automatically sends an
     * end-of-transmission (EOT) marker for this request.
     *
     * @param keys the message keys for routing/partitioning
     * @param datum the input message to process
     * @return a [Flow] of output [Message]s. Emit [Message.drop] to drop the message.
     */
    suspend fun processMessage(keys: List<String>, datum: Datum): Flow<Message>
}
