package io.numaproj.numaflowkt.sinker

/**
 * The result of processing a single [Datum].
 *
 * Each [Datum.id] must map to exactly one [Response] variant. The Numaflow platform
 * validates this correspondence downstream.
 *
 * Using a sealed class with data class subtypes gives:
 * - Compiler-enforced exhaustive `when` matching (no forgotten cases)
 * - Free `equals()`, `hashCode()`, `copy()`, `toString()` for test assertions
 *
 * [Serve] overrides `equals`/`hashCode` to compare [Serve.payload] by **content**,
 * not reference.
 *
 * Example:
 * ```kotlin
 * when (val response = processOne(datum)) {
 *     is Response.Ok        -> log.info("Success: ${response.id}")
 *     is Response.Failure   -> log.error("Failed ${response.id}: ${response.error}")
 *     is Response.Fallback  -> log.warn("Fallback: ${response.id}")
 *     is Response.Serve     -> log.info("Serve: ${response.payload.size} bytes")
 *     is Response.OnSuccess -> log.info("OnSuccess: ${response.id}")
 * }
 * ```
 */
sealed class Response {
    /** The [Datum.id] this response corresponds to. */
    abstract val id: String

    /** Message processed successfully. */
    data class Ok(override val id: String) : Response()

    /** Message processing failed with an error description. */
    data class Failure(override val id: String, val error: String) : Response()

    /** Message should be routed to the fallback sink. */
    data class Fallback(override val id: String) : Response()

    /**
     * Message should be served with the given payload (e.g. to a serving store).
     *
     * **ByteArray equality:** [equals] compares [payload] by content, not reference.
     */
    data class Serve(override val id: String, val payload: ByteArray) : Response() {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is Serve) return false
            return id == other.id && payload.contentEquals(other.payload)
        }

        override fun hashCode(): Int {
            var result = id.hashCode()
            result = 31 * result + payload.contentHashCode()
            return result
        }
    }

    /**
     * Message should be forwarded to the on-success sink.
     *
     * @property message The message to forward. If `null`, the original message is forwarded as-is.
     */
    data class OnSuccess(override val id: String, val message: Message? = null) : Response()
}
