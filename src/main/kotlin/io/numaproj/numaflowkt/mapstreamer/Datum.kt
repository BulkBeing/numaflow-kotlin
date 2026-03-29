package io.numaproj.numaflowkt.mapstreamer

import java.time.Instant

/**
 * Input message for a [MapStreamer].
 *
 * Identical structure to the [io.numaproj.numaflowkt.mapper.Datum] -- keys are passed
 * as a separate parameter to [MapStreamer.processMessage], not embedded in the Datum.
 * This matches the Java SDK convention. (Compare with
 * [io.numaproj.numaflowkt.batchmapper.Datum] which carries both `id` and `keys`.)
 *
 * **ByteArray equality:** [equals] and [hashCode] compare [value] by **content**
 * (not reference), so two Datums with identical byte payloads are considered equal.
 *
 * Example -- constructing test data:
 * ```kotlin
 * val datum = Datum(value = "hello world".toByteArray())
 * ```
 *
 * @property value     Raw message payload as bytes.
 * @property eventTime Event timestamp assigned by the source. `null` if not set.
 * @property watermark Watermark timestamp indicating processing progress. `null` if not set.
 * @property headers   Key-value metadata headers propagated through the pipeline. Empty map if none.
 */
data class Datum(
    val value: ByteArray,
    val eventTime: Instant? = null,
    val watermark: Instant? = null,
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
        result = 31 * result + (eventTime?.hashCode() ?: 0)
        result = 31 * result + (watermark?.hashCode() ?: 0)
        result = 31 * result + headers.hashCode()
        return result
    }
}
