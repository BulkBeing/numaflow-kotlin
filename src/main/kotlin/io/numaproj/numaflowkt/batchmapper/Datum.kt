package io.numaproj.numaflowkt.batchmapper

import java.time.Instant

/**
 * Input message for a [BatchMapper].
 *
 * Unlike [io.numaproj.numaflowkt.mapper.Datum] and [io.numaproj.numaflowkt.mapstreamer.Datum],
 * BatchMapper Datum includes [id] and [keys] because the batch processor needs to
 * correlate responses by ID (via [BatchResponse]) and has direct access to message keys.
 * This matches the Java SDK's `io.numaproj.numaflow.batchmapper.Datum` which exposes
 * `getId()` and `getKeys()` in addition to the standard payload fields.
 *
 * **ByteArray equality:** [equals] and [hashCode] compare [value] by **content**
 * (not reference), so two Datums with identical byte payloads are considered equal.
 *
 * Example -- constructing test data:
 * ```kotlin
 * val datum = Datum(id = "msg-1", value = "hello".toByteArray(), keys = listOf("key1"))
 * ```
 *
 * @property id        Unique message identifier assigned by the platform. Must be used
 *                     as the [BatchResponse.id] to correlate output with this input.
 * @property value     Raw message payload as bytes.
 * @property keys      Message keys for routing/partitioning. Empty list if not provided.
 * @property eventTime Event timestamp assigned by the source. `null` if not set.
 * @property watermark Watermark timestamp indicating processing progress. `null` if not set.
 * @property headers   Key-value metadata headers propagated through the pipeline. Empty map if none.
 */
data class Datum(
    val id: String,
    val value: ByteArray,
    val keys: List<String> = emptyList(),
    val eventTime: Instant? = null,
    val watermark: Instant? = null,
    val headers: Map<String, String> = emptyMap()
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Datum) return false
        return id == other.id
            && value.contentEquals(other.value)
            && keys == other.keys
            && eventTime == other.eventTime
            && watermark == other.watermark
            && headers == other.headers
    }

    override fun hashCode(): Int {
        var result = id.hashCode()
        result = 31 * result + value.contentHashCode()
        result = 31 * result + keys.hashCode()
        result = 31 * result + (eventTime?.hashCode() ?: 0)
        result = 31 * result + (watermark?.hashCode() ?: 0)
        result = 31 * result + headers.hashCode()
        return result
    }
}
