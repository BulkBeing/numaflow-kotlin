import io.numaproj.numaflowkt.sourcer.*
import kotlinx.coroutines.flow.flow
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * A simple in-memory source that generates numbered messages.
 *
 * Demonstrates thread-safe state management:
 * - [counter] uses AtomicInteger because read() can be called from multiple gRPC threads
 * - [unacked] uses ConcurrentHashMap.newKeySet() for safe concurrent access from ack/nack
 */
class SimpleSource : Sourcer {
    // Thread-safe counter -- gRPC may call read() from multiple Netty worker threads
    private val counter = AtomicInteger(0)

    // Track unacked offsets for pending count; concurrent because ack() and read()
    // may be called from different threads simultaneously
    private val unacked = ConcurrentHashMap.newKeySet<String>()

    override suspend fun read(request: ReadRequest) = flow {
        repeat(request.count.toInt()) {
            val i = counter.getAndIncrement()
            val offsetValue = i.toString()
            unacked.add(offsetValue)
            emit(Message(
                value = "message-$i".toByteArray(),
                offset = Offset(value = offsetValue.toByteArray()),
                eventTime = Instant.now(),
                keys = listOf("key-$i")
            ))
        }
    }

    override suspend fun ack(request: AckRequest) {
        // Remove acked offsets from the unacked set
        request.offsets.forEach { offset ->
            unacked.remove(String(offset.value))
        }
    }

    override suspend fun nack(request: NackRequest) {
        // In a real source, arrange for re-delivery of these offsets.
        // This simple example doesn't implement re-delivery.
    }

    override suspend fun getPending(): Long = unacked.size.toLong()

    override suspend fun getPartitions(): List<Int> = Sourcer.defaultPartitions()
}

fun main() {
    sourceServer {
        sourcer(SimpleSource())
    }.run()
}
