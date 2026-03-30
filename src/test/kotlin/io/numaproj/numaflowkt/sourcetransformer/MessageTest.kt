package io.numaproj.numaflowkt.sourcetransformer

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Instant

class MessageTest {

    private val now = Instant.now()

    @Test
    fun `message equality compares value by content`() {
        val m1 = Message(value = "hello".toByteArray(), eventTime = now, keys = listOf("k1"))
        val m2 = Message(value = "hello".toByteArray(), eventTime = now, keys = listOf("k1"))
        assertEquals(m1, m2)
        assertEquals(m1.hashCode(), m2.hashCode())
    }

    @Test
    fun `messages with different event times are not equal`() {
        val m1 = Message(value = "hello".toByteArray(), eventTime = Instant.ofEpochSecond(1))
        val m2 = Message(value = "hello".toByteArray(), eventTime = Instant.ofEpochSecond(2))
        assertNotEquals(m1, m2)
    }

    @Test
    fun `drop message has correct tags and event time`() {
        val drop = Message.drop(now)
        assertEquals(0, drop.value.size)
        assertEquals(now, drop.eventTime)
        assertEquals(listOf("U+005C__DROP__"), drop.tags)
    }

    @Test
    fun `message defaults are empty lists`() {
        val msg = Message(value = "test".toByteArray(), eventTime = now)
        assertEquals(emptyList<String>(), msg.keys)
        assertEquals(emptyList<String>(), msg.tags)
    }
}
