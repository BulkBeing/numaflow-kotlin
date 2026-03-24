package io.numaproj.numaflowkt.sinker

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class MessageTest {

    @Test
    fun `message equality compares value by content`() {
        val m1 = Message(value = "hello".toByteArray())
        val m2 = Message(value = "hello".toByteArray())
        assertEquals(m1, m2)
        assertEquals(m1.hashCode(), m2.hashCode())
    }

    @Test
    fun `messages with different values are not equal`() {
        val m1 = Message(value = "abc".toByteArray())
        val m2 = Message(value = "xyz".toByteArray())
        assertNotEquals(m1, m2)
    }

    @Test
    fun `message with key`() {
        val msg = Message(value = "data".toByteArray(), key = "routing-key")
        assertEquals("routing-key", msg.key)
    }

    @Test
    fun `message key defaults to empty string`() {
        val msg = Message(value = "data".toByteArray())
        assertEquals("", msg.key)
    }

    @Test
    fun `message with user metadata`() {
        val meta = UserMetadata()
        meta.put("g", "k", "v".toByteArray())

        val msg = Message(
            value = "data".toByteArray(),
            key = "k1",
            userMetadata = meta
        )
        assertEquals(meta, msg.userMetadata)
    }
}
