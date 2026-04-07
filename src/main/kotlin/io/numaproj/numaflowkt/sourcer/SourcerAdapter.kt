package io.numaproj.numaflowkt.sourcer

import io.numaproj.numaflowkt.sourcer.Sourcer as KotlinSourcer
import io.numaproj.numaflow.sourcer.Sourcer as JavaSourcer
import io.numaproj.numaflow.sourcer.ReadRequest as JavaReadRequest
import io.numaproj.numaflow.sourcer.AckRequest as JavaAckRequest
import io.numaproj.numaflow.sourcer.NackRequest as JavaNackRequest
import io.numaproj.numaflow.sourcer.OutputObserver as JavaOutputObserver
import io.numaproj.numaflow.sourcer.Offset as JavaOffset
import kotlinx.coroutines.runBlocking
import kotlin.time.toKotlinDuration

/**
 * Bridges the Kotlin [KotlinSourcer] to the Java SDK's [JavaSourcer].
 *
 * The Java SDK's gRPC Service calls these methods on Netty worker threads.
 * We use [runBlocking] to enter the coroutine world since these threads are
 * already dedicated to request processing.
 *
 * Error propagation: Exceptions thrown by the Kotlin sourcer propagate naturally
 * through [runBlocking] to the Java SDK's Service class, which handles error
 * logging and shutdown signaling. No try-catch is needed here.
 *
 * The sourcer differs from other modules (Mapper, Sinker, etc.) because:
 * - It does NOT use Akka actors. The Java SDK's Service.java calls the sourcer
 *   methods directly from the gRPC StreamObserver callbacks.
 * - It has 5 methods to bridge, not just one.
 * - `read()` uses Flow (collected here) instead of an OutputObserver on the Kotlin side.
 *
 * Type conversions are inlined here rather than in a separate Conversions.kt,
 * since the sourcer's conversions are straightforward offset/request mappings.
 */
internal class SourcerAdapter(private val sourcer: KotlinSourcer) : JavaSourcer() {

    override fun read(request: JavaReadRequest, observer: JavaOutputObserver) {
        runBlocking {
            val kotlinRequest = ReadRequest(
                count = request.count,
                timeout = request.timeout.toKotlinDuration()
            )
            // Collect the user's Flow and push each message to the Java observer.
            // This mirrors the MapStreamer adapter pattern.
            val flow = sourcer.read(kotlinRequest)
            flow.collect { message ->
                observer.send(message.toJavaMessage())
            }
        }
    }

    override fun ack(request: JavaAckRequest) {
        runBlocking {
            val offsets = request.offsets.map { it.toKotlinOffset() }
            sourcer.ack(AckRequest(offsets))
        }
    }

    override fun nack(request: JavaNackRequest) {
        runBlocking {
            val offsets = request.offsets.map { it.toKotlinOffset() }
            sourcer.nack(NackRequest(offsets))
        }
    }

    override fun getPending(): Long {
        return runBlocking {
            sourcer.getPending()
        }
    }

    override fun getPartitions(): List<Int> {
        return runBlocking {
            sourcer.getPartitions()
        }
    }

    // -- Inline type conversions --

    /**
     * Convert a Java SDK Offset to a Kotlin Offset.
     * Null-coalesces defensively: the Java SDK does not annotate nullability.
     */
    @Suppress("UNNECESSARY_SAFE_CALL")
    private fun JavaOffset.toKotlinOffset(): Offset = Offset(
        value = this.value ?: byteArrayOf(),
        partitionId = this.partitionId ?: 0
    )

    /**
     * Convert a Kotlin Message to a Java SDK Message.
     * Always uses the most-complete constructor to avoid fragile branching
     * based on which fields are empty.
     */
    private fun Message.toJavaMessage(): io.numaproj.numaflow.sourcer.Message {
        val javaOffset = JavaOffset(this.offset.value, this.offset.partitionId)
        return io.numaproj.numaflow.sourcer.Message(
            this.value,
            javaOffset,
            this.eventTime,
            this.keys.toTypedArray(),
            this.headers
        )
    }
}
