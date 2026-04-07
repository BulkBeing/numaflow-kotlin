package io.numaproj.numaflowkt.sourcer

import java.time.Instant

/**
 * A message read from a source.
 *
 * Produced by [Sourcer.read] and emitted via a [Flow]. Unlike mapper/sinker Messages,
 * sourcer Messages carry an [offset] (the position in the source) and an [eventTime]
 * (the event timestamp for watermarking).
 *
 * ByteArray equality: [equals] and [hashCode] compare [value] and [offset]'s value
 * by content, not reference.
 */
data class Message(
    val value: ByteArray,
    val offset: Offset,
    val eventTime: Instant,
    val keys: List<String> = emptyList(),
    val headers: Map<String, String> = emptyMap()
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Message) return false
        return value.contentEquals(other.value)
            && offset == other.offset
            && eventTime == other.eventTime
            && keys == other.keys
            && headers == other.headers
    }

    override fun hashCode(): Int {
        var result = value.contentHashCode()
        result = 31 * result + offset.hashCode()
        result = 31 * result + eventTime.hashCode()
        result = 31 * result + keys.hashCode()
        result = 31 * result + headers.hashCode()
        return result
    }
}
