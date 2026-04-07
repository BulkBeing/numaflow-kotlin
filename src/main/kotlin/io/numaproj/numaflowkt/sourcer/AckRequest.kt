package io.numaproj.numaflowkt.sourcer

/**
 * Request to acknowledge that the given offsets have been successfully processed.
 *
 * Wraps the offset list in a data class for forward-compatibility -- if the Java SDK
 * adds fields to its AckRequest interface in the future, this class can be extended
 * without breaking the [Sourcer.ack] signature.
 */
data class AckRequest(
    val offsets: List<Offset>
)
