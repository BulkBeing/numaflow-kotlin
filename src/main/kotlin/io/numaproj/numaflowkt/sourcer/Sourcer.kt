package io.numaproj.numaflowkt.sourcer

import kotlinx.coroutines.flow.Flow

/**
 * A user-defined source that reads messages from an external system.
 *
 * Unlike other Numaflow UDFs (Mapper, Sinker, etc.) which have a single
 * processing method, a Sourcer has multiple operations: reading, acknowledging,
 * negative-acknowledging, checking pending count, and reporting partitions.
 *
 * This is a regular interface (not `fun interface`) because it has multiple
 * abstract methods. Implement it as a class:
 *
 * ```kotlin
 * class MySource : Sourcer {
 *     override suspend fun read(request: ReadRequest): Flow<Message> = flow {
 *         // read messages and emit them
 *     }
 *     override suspend fun ack(request: AckRequest) { ... }
 *     override suspend fun nack(request: NackRequest) { ... }
 *     override suspend fun getPending(): Long = ...
 *     override suspend fun getPartitions(): List<Int> = Sourcer.defaultPartitions()
 * }
 * ```
 *
 * **Thread safety:** The sourcer methods may be called concurrently from multiple
 * gRPC worker threads. Implementations must use thread-safe data structures
 * (e.g., `AtomicInteger`, `ConcurrentHashMap`) for any shared mutable state.
 */
interface Sourcer {

    companion object {
        /**
         * Cached replica index from the NUMAFLOW_REPLICA env var.
         * Read once on first access; env vars don't change at runtime.
         */
        private val defaultPartition: Int by lazy {
            System.getenv("NUMAFLOW_REPLICA")?.toIntOrNull() ?: 0
        }

        /**
         * Returns the default partition list for sources without partitions.
         * Uses the `NUMAFLOW_REPLICA` env var (defaults to 0).
         *
         * Call this from [getPartitions] if the source does not have
         * meaningful partitions.
         */
        fun defaultPartitions(): List<Int> = listOf(defaultPartition)
    }

    /**
     * Read messages from the source.
     *
     * Implementations should return a [Flow] that emits up to [ReadRequest.count]
     * messages within [ReadRequest.timeout]. The implementation is responsible
     * for respecting these bounds -- the SDK does not enforce them.
     *
     * The SDK collects the flow and sends each message to the platform, then
     * sends the end-of-transmission marker automatically after the flow completes.
     */
    suspend fun read(request: ReadRequest): Flow<Message>

    /**
     * Acknowledge that the given offsets have been successfully processed.
     */
    suspend fun ack(request: AckRequest)

    /**
     * Negatively acknowledge the given offsets. The source should arrange for
     * these messages to be re-read on a subsequent [read] call.
     */
    suspend fun nack(request: NackRequest)

    /**
     * Return the number of pending (unread) messages at the source.
     * Return a negative value if pending information is not available.
     */
    suspend fun getPending(): Long

    /**
     * Return the partition IDs associated with this source.
     * Use [Sourcer.defaultPartitions] if the source does not have partitions.
     */
    suspend fun getPartitions(): List<Int>
}
