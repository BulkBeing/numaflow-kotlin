package io.numaproj.numaflowkt.sourcer

/**
 * Request to negatively acknowledge offsets, signaling that the messages should
 * be re-delivered on a subsequent [Sourcer.read] call.
 *
 * Wraps the offset list in a data class for forward-compatibility -- if the Java SDK
 * adds fields to its NackRequest interface in the future, this class can be extended
 * without breaking the [Sourcer.nack] signature.
 */
data class NackRequest(
    val offsets: List<Offset>
)
