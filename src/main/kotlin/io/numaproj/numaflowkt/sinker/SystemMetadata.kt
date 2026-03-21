package io.numaproj.numaflowkt.sinker

/**
 * Read-only system metadata organized as groups of key-value pairs.
 *
 * Structure: `group → key → ByteArray value`
 *
 * Provided by the Numaflow platform. Cannot be modified by user code.
 * This is the Kotlin counterpart of the Java SDK's `SystemMetadata`.
 *
 * Example:
 * ```kotlin
 * val traceId = datum.systemMetadata?.get("tracing", "trace-id")
 * ```
 */
class SystemMetadata(
    private val groups: Map<String, Map<String, ByteArray>>
) {
    /** Get a value by group and key. Returns `null` if the group or key is not found. */
    fun get(group: String, key: String): ByteArray? = groups[group]?.get(key)

    /** Get all group names. */
    fun groups(): Set<String> = groups.keys

    /** Get all keys within a group. Returns an empty set if the group doesn't exist. */
    fun keys(group: String): Set<String> = groups[group]?.keys ?: emptySet()
}
