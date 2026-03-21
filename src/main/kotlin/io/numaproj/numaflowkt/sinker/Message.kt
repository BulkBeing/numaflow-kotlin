package io.numaproj.numaflowkt.sinker

/**
 * A message to forward to the on-success sink.
 *
 * Used with [Response.OnSuccess] when the user wants to send a custom payload
 * instead of forwarding the original message.
 *
 * **ByteArray equality:** [equals] compares [value] by content, not reference.
 *
 * Example — forward original datum as-is:
 * ```kotlin
 * Response.OnSuccess(datum.id, Message.fromDatum(datum))
 * ```
 *
 * Example — custom message:
 * ```kotlin
 * Response.OnSuccess(datum.id, Message(value = transformedBytes))
 * ```
 *
 * @property value  Message payload as raw bytes.
 * @property keys   Optional message keys.
 * @property userMetadata  Optional user metadata to propagate.
 */
data class Message(
    val value: ByteArray,
    val keys: List<String>? = null,
    val userMetadata: UserMetadata? = null
) {
    companion object {
        /**
         * Creates a [Message] from an existing [Datum], copying its value, keys,
         * and user metadata.
         *
         * The [Datum.value] is defensively copied so mutations to the original
         * byte array do not affect this message.
         */
        fun fromDatum(datum: Datum): Message = Message(
            value = datum.value.copyOf(),
            keys = datum.keys,
            userMetadata = datum.userMetadata
        )
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Message) return false
        return value.contentEquals(other.value)
            && keys == other.keys
            && userMetadata == other.userMetadata
    }

    override fun hashCode(): Int {
        var result = value.contentHashCode()
        result = 31 * result + (keys?.hashCode() ?: 0)
        result = 31 * result + (userMetadata?.hashCode() ?: 0)
        return result
    }
}
