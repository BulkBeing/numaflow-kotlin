package io.numaproj.numaflowkt.mapstreamer

/**
 * Output message from a [MapStreamer].
 *
 * **ByteArray equality:** [equals] and [hashCode] compare [value] by **content**
 * (not reference), consistent with [Datum].
 *
 * To drop a message (filter it out), use the [drop] factory instead of constructing
 * a Message directly:
 * ```kotlin
 * emit(Message.drop())
 * ```
 *
 * @property value Raw message payload as bytes.
 * @property keys  Output keys for routing/partitioning. These may differ from the input
 *                 keys to re-route messages to different partitions. Empty list if not set.
 * @property tags  Tags for conditional forwarding in Numaflow pipelines. The platform uses
 *                 tags to decide which downstream vertices receive this message. Empty list
 *                 if not set (message goes to all downstream vertices).
 */
data class Message(
    val value: ByteArray,
    val keys: List<String> = emptyList(),
    val tags: List<String> = emptyList()
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Message) return false
        return value.contentEquals(other.value)
            && keys == other.keys
            && tags == other.tags
    }

    override fun hashCode(): Int {
        var result = value.contentHashCode()
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
         * Create a message that will be dropped (filtered out) by the platform.
         *
         * The returned message carries a special sentinel tag that the Numaflow
         * platform recognizes. The message will not be forwarded to any downstream vertex.
         */
        fun drop(): Message = Message(value = byteArrayOf(), tags = DROP_TAGS)
    }
}
