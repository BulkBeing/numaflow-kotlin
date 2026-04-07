package io.numaproj.numaflowkt.sourcer

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Instant

class MessageTest {

    private val now = Instant.now()
    private val offset = Offset(value = "0".toByteArray())

    @Test
    fun `message equality compares value by content`() {
        val m1 = Message(value = "hello".toByteArray(), offset = offset, eventTime = now)
        val m2 = Message(value = "hello".toByteArray(), offset = offset, eventTime = now)
        assertEquals(m1, m2)
        assertEquals(m1.hashCode(), m2.hashCode())
    }

    @Test
    fun `messages with different content are not equal`() {
        val m1 = Message(value = "hello".toByteArray(), offset = offset, eventTime = now)
        val m2 = Message(value = "world".toByteArray(), offset = offset, eventTime = now)
        assertNotEquals(m1, m2)
    }

    @Test
    fun `messages with different keys are not equal`() {
        val m1 = Message(value = "hello".toByteArray(), offset = offset, eventTime = now, keys = listOf("k1"))
        val m2 = Message(value = "hello".toByteArray(), offset = offset, eventTime = now, keys = listOf("k2"))
        assertNotEquals(m1, m2)
    }

    @Test
    fun `messages with different headers are not equal`() {
        val m1 = Message(value = "hello".toByteArray(), offset = offset, eventTime = now, headers = mapOf("a" to "1"))
        val m2 = Message(value = "hello".toByteArray(), offset = offset, eventTime = now, headers = mapOf("b" to "2"))
        assertNotEquals(m1, m2)
    }

    @Test
    fun `message defaults are empty`() {
        val msg = Message(value = "test".toByteArray(), offset = offset, eventTime = now)
        assertEquals(emptyList<String>(), msg.keys)
        assertEquals(emptyMap<String, String>(), msg.headers)
    }
}
