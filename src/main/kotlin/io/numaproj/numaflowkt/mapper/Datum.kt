package io.numaproj.numaflowkt.mapper

import java.time.Instant

/**
 * Input message for a [Mapper].
 *
 * Keys are passed as a separate parameter to [Mapper.processMessage],
 * not embedded in the Datum. This matches the Java SDK convention where
 * `Mapper.processMessage(String[] keys, Datum datum)` separates routing
 * keys from the message payload. (Compare with
 * [io.numaproj.numaflowkt.batchmapper.Datum] which carries both `id` and `keys`.)
 *
 * **ByteArray equality:** [equals] and [hashCode] compare [value] by **content**
 * (not reference), so two Datums with identical byte payloads are considered equal.
 * This avoids the well-known Kotlin pitfall where `data class` uses reference equality
 * for arrays.
 *
 * Example -- constructing test data:
 * ```kotlin
 * val datum = Datum(value = "hello".toByteArray(), eventTime = Instant.now(), watermark = Instant.now())
 * ```
 *
 * @property value     Raw message payload as bytes.
 * @property eventTime Event timestamp assigned by the source.
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
