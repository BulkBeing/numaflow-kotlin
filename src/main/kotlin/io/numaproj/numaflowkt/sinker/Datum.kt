package io.numaproj.numaflowkt.sinker

import java.time.Instant

/**
 * An incoming message in a Numaflow pipeline.
 *
 * Only [id] and [value] are required. Collection fields default to empty,
 * timestamp fields default to `null`.
 *
 * **ByteArray equality:** [equals] and [hashCode] compare [value] by **content**
 * (not reference), so two Datums with identical byte payloads are considered equal.
 * This avoids the well-known Kotlin pitfall where `data class` uses reference equality
 * for arrays.
 *
 * Example — constructing test data:
 * ```kotlin
 * val datum = Datum(id = "msg-1", value = "hello".toByteArray())
 * ```
 *
 * @property id        Unique message identifier. Used to correlate [Response] to input.
 * @property value     Message payload as raw bytes.
 * @property keys      Message keys for routing/partitioning. Empty if not provided.
 * @property eventTime Optional event timestamp.
 * @property watermark Optional watermark timestamp.
 * @property headers   Key-value headers. Empty if not provided.
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
