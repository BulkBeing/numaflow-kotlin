package io.numaproj.numaflowkt.sinker

/**
 * Mutable user-defined metadata organized as groups of key-value pairs.
 *
 * Structure: `group → key → ByteArray value`
 *
 * This is the Kotlin counterpart of the Java SDK's `UserMetadata`. Values are
 * stored as raw byte arrays, matching the protobuf wire format.
 *
 * Example:
 * ```kotlin
 * val meta = UserMetadata()
 * meta.put("tracking", "request-id", "abc-123".toByteArray())
 * val requestId = meta.get("tracking", "request-id") // ByteArray
 * ```
 */
class UserMetadata(
    private val groups: MutableMap<String, MutableMap<String, ByteArray>> = mutableMapOf()
) {
    /** Get a value by group and key. Returns `null` if the group or key is not found. */
    fun get(group: String, key: String): ByteArray? = groups[group]?.get(key)

    /** Set a value for a group and key. Creates the group if it doesn't exist. */
    fun put(group: String, key: String, value: ByteArray) {
        groups.getOrPut(group) { mutableMapOf() }[key] = value
    }

    /** Remove a key from a group. No-op if the group or key doesn't exist. */
    fun remove(group: String, key: String) {
        groups[group]?.remove(key)
    }

    /** Remove an entire group and all its keys. */
    fun removeGroup(group: String) {
        groups.remove(group)
    }

    /** Get all group names. */
    fun groups(): Set<String> = groups.keys

    /** Get all keys within a group. Returns an empty set if the group doesn't exist. */
    fun keys(group: String): Set<String> = groups[group]?.keys ?: emptySet()

    /** Clear all metadata. */
    fun clear() {
        groups.clear()
    }

    /**
     * Returns the raw internal map. Used by the internal bridge layer to
     * convert to Java SDK types. Not part of the public API contract.
     */
    internal fun toMap(): Map<String, Map<String, ByteArray>> = groups

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is UserMetadata) return false
        // Compare group structure and byte array contents
        val thisGroups = this.groups
        val otherGroups = other.groups
        if (thisGroups.keys != otherGroups.keys) return false
        for ((group, thisKeys) in thisGroups) {
            val otherKeys = otherGroups[group] ?: return false
            if (thisKeys.keys != otherKeys.keys) return false
            for ((key, thisValue) in thisKeys) {
                val otherValue = otherKeys[key] ?: return false
                if (!thisValue.contentEquals(otherValue)) return false
            }
        }
        return true
    }

    override fun hashCode(): Int {
        var result = 0
        for ((group, keys) in groups) {
            result = 31 * result + group.hashCode()
            for ((key, value) in keys) {
                result = 31 * result + key.hashCode()
                result = 31 * result + value.contentHashCode()
            }
        }
        return result
    }
}
