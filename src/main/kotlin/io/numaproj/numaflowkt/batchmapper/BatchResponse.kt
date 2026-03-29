package io.numaproj.numaflowkt.batchmapper

/**
 * Response for a single input message in a batch.
 *
 * Each [BatchResponse] correlates to an input [Datum] by [id]. The Numaflow platform
 * uses this ID-based correlation to track which input messages have been processed and
 * what output was produced for each. Every input [Datum] must have a corresponding
 * [BatchResponse] with a matching [id].
 *
 * Example -- single output per input:
 * ```kotlin
 * BatchResponse(
 *     id = datum.id,
 *     messages = listOf(Message(value = transformed.toByteArray(), keys = datum.keys))
 * )
 * ```
 *
 * Example -- drop (filter out) an input:
 * ```kotlin
 * BatchResponse(id = datum.id, messages = listOf(Message.drop()))
 * ```
 *
 * @property id       The ID of the input [Datum] this response corresponds to. Must
 *                    match [Datum.id] exactly.
 * @property messages The output messages for this input. Use `listOf(Message.drop())`
 *                    to indicate the message should be dropped.
 */
data class BatchResponse(
    val id: String,
    val messages: List<Message> = emptyList()
)
