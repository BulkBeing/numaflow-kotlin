package io.numaproj.numaflowkt.sinker

import java.time.Instant

/**
 * An incoming message in a Numaflow pipeline.
 *
 * Only [id] and [value] are required. All other fields default to `null` when not
 * provided by the platform or when constructing test data.
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
 * @property id      Unique message identifier. Used to correlate [Response] to input.
 * @property value   Message payload as raw bytes.
 * @property keys    Optional message keys for routing/partitioning.
 * @property eventTime  Optional event timestamp.
 * @property watermark  Optional watermark timestamp.
 * @property headers    Optional key-value headers.
 * @property userMetadata  Optional mutable user-defined metadata.
 * @property systemMetadata  Optional read-only system metadata.
 */
data class Datum(
    val id: String,
    val value: ByteArray,
    val keys: List<String>? = null,
    val eventTime: Instant? = null,
    val watermark: Instant? = null,
    val headers: Map<String, String>? = null,
    val userMetadata: UserMetadata? = null,
    val systemMetadata: SystemMetadata? = null
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
            && userMetadata == other.userMetadata
            && systemMetadata == other.systemMetadata
    }

    override fun hashCode(): Int {
        var result = id.hashCode()
        result = 31 * result + value.contentHashCode()
        result = 31 * result + (keys?.hashCode() ?: 0)
        result = 31 * result + (eventTime?.hashCode() ?: 0)
        result = 31 * result + (watermark?.hashCode() ?: 0)
        result = 31 * result + (headers?.hashCode() ?: 0)
        result = 31 * result + (userMetadata?.hashCode() ?: 0)
        result = 31 * result + (systemMetadata?.hashCode() ?: 0)
        return result
    }
}
