package io.numaproj.numaflowkt.sourcetransformer

/**
 * A user-defined source transform function.
 *
 * Source transformers run at the source vertex and can both transform
 * messages and reassign their event times. This is the key difference
 * from a regular [io.numaproj.numaflowkt.mapper.Mapper] — each output
 * [Message] carries an [eventTime][Message.eventTime] that becomes the
 * message's new event time in the pipeline.
 *
 * Because this is a `fun interface`, simple transformers can be written
 * as lambdas:
 *
 * ```kotlin
 * val transformer = SourceTransformer { keys, datum ->
 *     listOf(Message(
 *         value = datum.value,
 *         eventTime = datum.eventTime,
 *         keys = keys
 *     ))
 * }
 * ```
 *
 * @see sourceTransformerServer for wiring a SourceTransformer into a gRPC server
 */
fun interface SourceTransformer {
    /**
     * Process a single message and return output messages.
     *
     * Each returned [Message] must include an [eventTime][Message.eventTime],
     * which the Numaflow platform uses as the message's event time for
     * watermark progression.
     *
     * This is a `suspend` function, so implementations can freely call other
     * suspending functions (HTTP clients, database queries, etc.) and launch
     * child coroutines via [kotlinx.coroutines.coroutineScope].
     *
     * @param keys the message keys for routing/partitioning
     * @param datum the input message to process
     * @return list of output [Message]s. Return `listOf(Message.drop(eventTime))` to drop.
     */
    suspend fun processMessage(keys: List<String>, datum: Datum): List<Message>
}
