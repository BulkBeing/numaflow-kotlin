package io.numaproj.numaflowkt.sinker

import kotlinx.coroutines.flow.Flow

/**
 * A user-defined sink that processes batches of messages from a Numaflow pipeline.
 *
 * The SDK delivers messages as a [Flow] of [Datum]. The implementation must return
 * exactly one [Response] per input [Datum], identified by [Datum.id].
 *
 * Because this is a `fun interface`, simple sinks can be written as lambdas:
 *
 * ```kotlin
 * val mySink = Sinker { messages ->
 *     messages.map { datum ->
 *         try {
 *             writeToExternalSystem(datum.value)
 *             Response.Ok(datum.id)
 *         } catch (e: Exception) {
 *             Response.Failure(datum.id, e.message ?: "unknown error")
 *         }
 *     }.toList()
 * }
 * ```
 *
 * For batch processing, collect the flow first, then process in bulk:
 *
 * ```kotlin
 * val batchSink = Sinker { messages ->
 *     val datums = messages.toList()
 *     val results = bulkInsert(datums.map { it.value })
 *     datums.zip(results).map { (datum, success) ->
 *         if (success) Response.Ok(datum.id)
 *         else Response.Failure(datum.id, "insert failed")
 *     }
 * }
 * ```
 */
fun interface Sinker {
    /**
     * Process a batch of messages delivered as a [Flow].
     *
     * This is a `suspend` function, so implementations can freely call other
     * suspending functions (database clients, HTTP calls, etc.) and launch
     * child coroutines via [kotlinx.coroutines.coroutineScope].
     *
     * @param messages a [Flow] of [Datum] representing the current batch
     * @return one [Response] per input [Datum], correlated by [Datum.id]
     */
    suspend fun processMessages(messages: Flow<Datum>): List<Response>
}
