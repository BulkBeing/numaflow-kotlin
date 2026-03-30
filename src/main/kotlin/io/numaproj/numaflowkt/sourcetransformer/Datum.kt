package io.numaproj.numaflowkt.sourcetransformer

import java.time.Instant

/**
 * Input message for a [SourceTransformer].
 *
 * Keys are passed as a separate parameter to [SourceTransformer.processMessage],
 * not embedded in the Datum. This matches the Java SDK convention where
 * `SourceTransformer.processMessage(String[] keys, Datum datum)` separates routing
 * keys from the message payload.
 *
 * The [eventTime] here is the *original* event time assigned by the source.
 * The transformer can reassign it by setting a different event time on each
 * output [Message].
 *
 * **ByteArray equality:** [equals] and [hashCode] compare [value] by **content**
 * (not reference), so two Datums with identical byte payloads are considered equal.
 * This avoids the well-known Kotlin pitfall where `data class` uses reference equality
 * for arrays.
 *
 * @property value     Raw message payload as bytes.
 * @property eventTime Original event timestamp from the source. The transformer can
 *                     reassign it by returning a different value on output [Message]s.
 * @property watermark Watermark timestamp indicating processing progress.
 * @property headers   Key-value metadata headers propagated through the pipeline. Empty map if none.
 */
data class Datum(
    val value: ByteArray,
    val eventTime: Instant,
    val watermark: Instant,
    val headers: Map<String, String> = emptyMap()
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Datum) return false
        return value.contentEquals(other.value)
            && eventTime == other.eventTime
            && watermark == other.watermark
            && headers == other.headers
    }

    override fun hashCode(): Int {
        var result = value.contentHashCode()
        result = 31 * result + eventTime.hashCode()
        result = 31 * result + watermark.hashCode()
        result = 31 * result + headers.hashCode()
        return result
    }
}
