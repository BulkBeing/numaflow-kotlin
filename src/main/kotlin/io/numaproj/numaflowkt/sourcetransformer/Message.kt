package io.numaproj.numaflowkt.sourcetransformer

import java.time.Instant

/**
 * Output message from a [SourceTransformer].
 *
 * Unlike [io.numaproj.numaflowkt.mapper.Message], source transformer messages
 * carry an [eventTime] that the Numaflow platform uses as the message's event
 * time for watermark progression. This is the primary purpose of the source
 * transform vertex — to assign (or reassign) event times.
 *
 * **ByteArray equality:** [equals] and [hashCode] compare [value] by content.
 *
 * To drop a message, use [drop]:
 * ```kotlin
 * listOf(Message.drop(datum.eventTime))
 * ```
 *
 * @property value     Raw message payload as bytes.
 * @property eventTime Event time to assign to this message. Required — even
 *                     dropped messages need an event time for watermark calculation.
 * @property keys      Output keys for routing/partitioning. Empty list if not set.
 * @property tags      Tags for conditional forwarding. Empty list if not set.
 */
data class Message(
    val value: ByteArray,
    val eventTime: Instant,
    val keys: List<String> = emptyList(),
    val tags: List<String> = emptyList()
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Message) return false
        return value.contentEquals(other.value)
            && eventTime == other.eventTime
            && keys == other.keys
            && tags == other.tags
    }

    override fun hashCode(): Int {
        var result = value.contentHashCode()
        result = 31 * result + eventTime.hashCode()
        result = 31 * result + keys.hashCode()
        result = 31 * result + tags.hashCode()
        return result
    }

    companion object {
        /**
         * Sentinel tag recognized by the Numaflow platform as "drop this message".
         * Must match the Java SDK's `Message.DROP_TAG` value exactly.
         */
        private val DROP_TAGS = listOf("U+005C__DROP__")

        /**
         * Create a message that will be dropped by the platform.
         *
         * Even dropped messages require an [eventTime] because the platform
         * counts them toward watermark calculations. The event time should
         * typically be the original event time from the input [Datum].
         *
         * @param eventTime event time for watermark calculation
         */
        fun drop(eventTime: Instant): Message =
            Message(value = byteArrayOf(), eventTime = eventTime, tags = DROP_TAGS)
    }
}
