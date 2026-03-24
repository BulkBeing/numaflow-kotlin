package io.numaproj.numaflowkt.sinker

/**
 * A message to forward to the on-success sink.
 *
 * Used with [Response.OnSuccess] to specify what payload to forward.
 *
 * **ByteArray equality:** [equals] compares [value] by content, not reference.
 *
 * Example — forward original datum payload:
 * ```kotlin
 * Response.OnSuccess(datum.id, Message(value = datum.value))
 * ```
 *
 * Example — custom message with key:
 * ```kotlin
 * Response.OnSuccess(datum.id, Message(
 *     value = transformedBytes,
 *     key = "routing-key"
 * ))
 * ```
 *
 * Example — with metadata:
 * ```kotlin
 * Response.OnSuccess(datum.id, Message(
 *     value = datum.value,
 *     key = "routing-key",
 *     userMetadata = UserMetadata().apply {
 *         put("tracking", "request-id", "abc-123".toByteArray())
 *     }
 * ))
 * ```
 *
 * @property value         Message payload as raw bytes.
 * @property key           Message key for routing. Empty string if not provided.
 *                         Maps to the Java SDK's `Message.key` (singular).
 * @property userMetadata  Optional user metadata to propagate to the on-success sink.
 */
data class Message(
    val value: ByteArray,
    val key: String = "",
    val userMetadata: UserMetadata? = null
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Message) return false
        return value.contentEquals(other.value)
            && key == other.key
            && userMetadata == other.userMetadata
    }

    override fun hashCode(): Int {
        var result = value.contentHashCode()
        result = 31 * result + key.hashCode()
        result = 31 * result + (userMetadata?.hashCode() ?: 0)
        return result
    }
}
