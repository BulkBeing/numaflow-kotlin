package io.numaproj.numaflowkt.sourcer

import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.seconds

class SourcerUnitTest {

    private val now = Instant.now()

    @Test
    fun `read returns flow of messages`() = runTest {
        val sourcer = TestSource(generateMessages(5))
        val messages = sourcer.read(ReadRequest(count = 5, timeout = 1.seconds)).toList()
        assertEquals(5, messages.size)
        assertEquals("message-0", String(messages[0].value))
        assertEquals("message-4", String(messages[4].value))
    }

    @Test
    fun `read respects count limit`() = runTest {
        val sourcer = TestSource(generateMessages(10))
        val messages = sourcer.read(ReadRequest(count = 3, timeout = 1.seconds)).toList()
        assertEquals(3, messages.size)
    }

    @Test
    fun `ack removes from pending`() = runTest {
        val sourcer = TestSource(generateMessages(5))
        sourcer.read(ReadRequest(count = 5, timeout = 1.seconds)).toList()
        assertEquals(5, sourcer.getPending())

        val offsets = (0 until 5).map { Offset(value = it.toString().toByteArray()) }
        sourcer.ack(AckRequest(offsets))
        assertEquals(0, sourcer.getPending())
    }

    @Test
    fun `nack resets read index for re-read`() = runTest {
        val sourcer = TestSource(generateMessages(5))
        val batch = sourcer.read(ReadRequest(count = 5, timeout = 1.seconds)).toList()
        assertEquals(5, batch.size)

        sourcer.nack(NackRequest(batch.map { it.offset }))

        val rebatch = sourcer.read(ReadRequest(count = 5, timeout = 1.seconds)).toList()
        assertEquals(5, rebatch.size)
        assertEquals("message-0", String(rebatch[0].value))
    }

    @Test
    fun `getPartitions returns default partitions`() = runTest {
        val sourcer = TestSource(emptyList())
        assertEquals(Sourcer.defaultPartitions(), sourcer.getPartitions())
    }

    // -- Test helper --

    private class TestSource(
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
                eventTime = now
            )
        }
}
