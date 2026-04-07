package io.numaproj.numaflowkt.sourcer

import kotlinx.coroutines.flow.flow
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.seconds

class SourcerTestKitTest {

    @Test
    fun `read ack lifecycle over gRPC`() {
        val source = InMemorySource(messages = generateMessages(10))
        SourcerTestKit(source, SourcerConfig(port = 50111)).use { kit ->
            kit.start()

            // Read 5 messages
            val batch1 = kit.read(count = 5)
            assertEquals(5, batch1.size)

            // Pending is total minus acked (nothing acked yet)
            assertEquals(10, kit.getPending())

            // Ack first batch
            kit.ack(batch1.map { it.offset })
            assertEquals(5, kit.getPending())

            // Read remaining 5
            val batch2 = kit.read(count = 5)
            assertEquals(5, batch2.size)

            // Ack second batch
            kit.ack(batch2.map { it.offset })
            assertEquals(0, kit.getPending())

            // Check partitions
            assertEquals(listOf(0), kit.getPartitions())
        }
    }

    @Test
    fun `nack causes re-read`() {
        val source = InMemorySource(messages = generateMessages(5))
        SourcerTestKit(source, SourcerConfig(port = 50112)).use { kit ->
            kit.start()

            val batch = kit.read(count = 5)
            assertEquals(5, batch.size)

            // Nack all
            kit.nack(batch.map { it.offset })

            // Re-read should get same messages
            val rebatch = kit.read(count = 5)
            assertEquals(5, rebatch.size)
        }
    }

    // -- Private test helper --

    private class InMemorySource(
        private val messages: List<Message>
    ) : Sourcer {
        private val readIndex = AtomicInteger(0)
        private val acked = ConcurrentHashMap.newKeySet<String>()

        override suspend fun read(request: ReadRequest) = flow {
            val count = minOf(request.count.toInt(), messages.size - readIndex.get())
            repeat(count) {
                val idx = readIndex.getAndIncrement()
                if (idx < messages.size) emit(messages[idx])
            }
        }

        override suspend fun ack(request: AckRequest) {
            request.offsets.forEach { acked.add(String(it.value)) }
        }

        override suspend fun nack(request: NackRequest) {
            request.offsets.forEach { offset ->
                val idx = String(offset.value).toIntOrNull()
                if (idx != null) {
                    readIndex.updateAndGet { current -> minOf(current, idx) }
                }
            }
        }

        override suspend fun getPending(): Long =
            (messages.size - acked.size).toLong()

        override suspend fun getPartitions(): List<Int> =
            Sourcer.defaultPartitions()
    }

    private fun generateMessages(count: Int): List<Message> =
        (0 until count).map { i ->
            Message(
                value = "message-$i".toByteArray(),
                offset = Offset(value = i.toString().toByteArray()),
                eventTime = Instant.now()
            )
        }
}
