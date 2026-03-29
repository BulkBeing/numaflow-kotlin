package io.numaproj.numaflowkt.batchmapper

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class MessageTest {

    @Test
    fun `message equality compares value by content`() {
        val m1 = Message(value = "hello".toByteArray(), keys = listOf("k1"))
        val m2 = Message(value = "hello".toByteArray(), keys = listOf("k1"))
        assertEquals(m1, m2)
        assertEquals(m1.hashCode(), m2.hashCode())
    }

    @Test
    fun `drop message has correct tags`() {
        val drop = Message.drop()
        assertEquals(byteArrayOf().size, drop.value.size)
        assertEquals(listOf("U+005C__DROP__"), drop.tags)
    }

    @Test
    fun `message defaults are empty lists`() {
        val msg = Message(value = "test".toByteArray())
        assertEquals(emptyList<String>(), msg.keys)
        assertEquals(emptyList<String>(), msg.tags)
    }
}
