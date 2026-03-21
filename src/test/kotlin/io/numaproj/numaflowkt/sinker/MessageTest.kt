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
    fun `fromDatum copies value defensively`() {
        val original = "hello".toByteArray()
        val datum = Datum(id = "1", value = original)
        val message = Message.fromDatum(datum)

        // Mutate the original — should not affect the message
        original[0] = 'X'.code.toByte()
        assertArrayEquals("hello".toByteArray(), message.value)
    }

    @Test
    fun `fromDatum preserves keys and metadata`() {
        val meta = UserMetadata()
        meta.put("g", "k", "v".toByteArray())

        val datum = Datum(
            id = "1",
            value = "data".toByteArray(),
            keys = listOf("k1", "k2"),
            userMetadata = meta
        )

        val message = Message.fromDatum(datum)
        assertEquals(listOf("k1", "k2"), message.keys)
        assertEquals(meta, message.userMetadata)
    }
}
