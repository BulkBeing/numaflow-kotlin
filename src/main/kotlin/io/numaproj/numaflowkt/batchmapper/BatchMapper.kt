package io.numaproj.numaflowkt.batchmapper

import kotlinx.coroutines.flow.Flow

/**
 * A user-defined batch map function that processes multiple messages at once.
 *
 * Unlike the per-message [io.numaproj.numaflowkt.mapper.Mapper], BatchMapper receives
 * a batch of messages as a [Flow] and must return a [BatchResponse] for each input
 * [Datum], correlated by [Datum.id]. This enables batch optimizations like bulk
 * database lookups or vectorized processing.
 *
 * The pattern is similar to [io.numaproj.numaflowkt.sinker.Sinker]: a `Flow<Datum>` in,
 * a list of correlated responses out. The key difference is that the Sinker returns
 * success/failure status while BatchMapper returns transformed output messages.
 *
 * Because this is a `fun interface`, simple batch mappers can be written as lambdas:
 *
 * Example -- batch enrichment (collect first, then bulk process):
 * ```kotlin
 * val enricher = BatchMapper { messages ->
 *     val datums = messages.toList()
 *     val enrichments = bulkLookup(datums.map { String(it.value) })
 *     datums.mapIndexed { i, datum ->
 *         BatchResponse(
 *             id = datum.id,
 *             messages = listOf(Message(value = enrichments[i].toByteArray(), keys = datum.keys))
 *         )
 *     }
 * }
 * ```
 *
 * Example -- simple 1:1 batch (stream-process each message):
 * ```kotlin
 * val upper = BatchMapper { messages ->
 *     messages.map { datum ->
 *         BatchResponse(
 *             id = datum.id,
 *             messages = listOf(Message(
 *                 value = String(datum.value).uppercase().toByteArray(),
 *                 keys = datum.keys
 *             ))
 *         )
 *     }.toList()
 * }
 * ```
 *
 * @see batchMapServer for wiring a BatchMapper into a gRPC server
 * @see io.numaproj.numaflowkt.mapper.Mapper for per-message (non-batch) processing
 * @see io.numaproj.numaflowkt.mapstreamer.MapStreamer for streaming output
 */
fun interface BatchMapper {
    /**
     * Process a batch of messages.
     *
     * This is a `suspend` function, so implementations can freely call other
     * suspending functions (bulk database inserts, HTTP calls, etc.) and launch
     * child coroutines via [kotlinx.coroutines.coroutineScope].
     *
     * **Important:** Each input [Datum] has a unique [Datum.id]. The returned list
     * must contain exactly one [BatchResponse] per input datum, with the [BatchResponse.id]
     * matching the corresponding [Datum.id]. The Numaflow platform uses this correlation
     * to track processing status.
     *
     * @param messages a [Flow] of [Datum] representing the current batch.
     *   The flow is backed by the Java SDK's `DatumIterator` which blocks until
     *   the next element is available and completes when the batch boundary is reached.
     * @return one [BatchResponse] per input datum, correlated by ID.
     */
    suspend fun processMessage(messages: Flow<Datum>): List<BatchResponse>
}
