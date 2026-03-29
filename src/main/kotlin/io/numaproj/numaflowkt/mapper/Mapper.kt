package io.numaproj.numaflowkt.mapper

/**
 * A user-defined map function that transforms messages one at a time.
 *
 * Each invocation receives a single message (identified by [keys] and [datum])
 * and returns zero or more output [Message]s. The SDK handles concurrency
 * internally via the Java SDK's Akka actor system -- each call may run on a
 * different coroutine.
 *
 * Because this is a `fun interface`, simple mappers can be written as lambdas:
 *
 * Example -- simple 1:1 transformation:
 * ```kotlin
 * val mapper = Mapper { keys, datum ->
 *     val upper = String(datum.value).uppercase()
 *     listOf(Message(value = upper.toByteArray(), keys = keys))
 * }
 * ```
 *
 * Example -- flatmap (1:N):
 * ```kotlin
 * val splitter = Mapper { keys, datum ->
 *     String(datum.value).split(",").map { word ->
 *         Message(value = word.trim().toByteArray(), keys = keys)
 *     }
 * }
 * ```
 *
 * Example -- filter (drop messages):
 * ```kotlin
 * val filter = Mapper { keys, datum ->
 *     if (String(datum.value).startsWith("keep"))
 *         listOf(Message(value = datum.value, keys = keys))
 *     else
 *         listOf(Message.drop())
 * }
 * ```
 *
 * @see mapServer for wiring a Mapper into a gRPC server
 * @see io.numaproj.numaflowkt.mapstreamer.MapStreamer for streaming (incremental) output
 * @see io.numaproj.numaflowkt.batchmapper.BatchMapper for batch processing
 */
fun interface Mapper {
    /**
     * Process a single message and return output messages.
     *
     * This is a `suspend` function, so implementations can freely call other
     * suspending functions (HTTP clients, database queries, etc.) and launch
     * child coroutines via [kotlinx.coroutines.coroutineScope].
     *
     * @param keys the message keys for routing/partitioning
     * @param datum the input message to process
     * @return list of output [Message]s. Return `listOf(Message.drop())` to drop the message.
     */
    suspend fun processMessage(keys: List<String>, datum: Datum): List<Message>
}
