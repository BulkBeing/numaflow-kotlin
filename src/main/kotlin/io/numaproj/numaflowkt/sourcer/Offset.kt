package io.numaproj.numaflowkt.sourcer

/**
 * Identifies a position within a source partition.
 *
 * The [value] is opaque bytes whose meaning is defined by the source implementation
 * (e.g., a Kafka offset serialized as bytes, a sequence number, etc.).
 *
 * [partitionId] defaults to the replica index from the NUMAFLOW_REPLICA env var
 * (via [Sourcer.defaultPartitions]), which is the common case for single-partition
 * sources. Multi-partition sources should specify [partitionId] explicitly.
 *
 * ByteArray equality: [equals] and [hashCode] compare [value] by content, not reference.
 */
data class Offset(
    val value: ByteArray,
    val partitionId: Int = Sourcer.defaultPartitions().first()
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Offset) return false
        return value.contentEquals(other.value) && partitionId == other.partitionId
    }

    override fun hashCode(): Int {
        var result = value.contentHashCode()
        result = 31 * result + partitionId
        return result
    }
}
