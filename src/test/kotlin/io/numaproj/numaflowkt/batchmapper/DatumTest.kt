package io.numaproj.numaflowkt.batchmapper

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Instant

class DatumTest {

    @Test
    fun `datum equality compares value by content`() {
        val d1 = Datum(id = "1", value = "hello".toByteArray())
        val d2 = Datum(id = "1", value = "hello".toByteArray())
        assertEquals(d1, d2)
        assertEquals(d1.hashCode(), d2.hashCode())
    }

    @Test
    fun `datums with different values are not equal`() {
        val d1 = Datum(id = "1", value = "hello".toByteArray())
        val d2 = Datum(id = "1", value = "world".toByteArray())
        assertNotEquals(d1, d2)
    }

    @Test
    fun `id included in equality`() {
        val d1 = Datum(id = "1", value = "hello".toByteArray())
        val d2 = Datum(id = "2", value = "hello".toByteArray())
        assertNotEquals(d1, d2)
    }

    @Test
    fun `keys included in equality`() {
        val d1 = Datum(id = "1", value = "hello".toByteArray(), keys = listOf("k1"))
        val d2 = Datum(id = "1", value = "hello".toByteArray(), keys = listOf("k2"))
        assertNotEquals(d1, d2)
    }

    @Test
    fun `datum defaults are empty collections and null timestamps`() {
        val datum = Datum(id = "1", value = byteArrayOf())
        assertEquals(emptyList<String>(), datum.keys)
        assertNull(datum.eventTime)
        assertNull(datum.watermark)
        assertEquals(emptyMap<String, String>(), datum.headers)
    }

    @Test
    fun `datum with all fields populated`() {
        val now = Instant.now()
        val datum = Datum(
            id = "msg-1",
            value = "payload".toByteArray(),
            keys = listOf("key1", "key2"),
            eventTime = now,
            watermark = now,
            headers = mapOf("h1" to "v1")
        )
        assertEquals("msg-1", datum.id)
        assertArrayEquals("payload".toByteArray(), datum.value)
        assertEquals(listOf("key1", "key2"), datum.keys)
        assertEquals(now, datum.eventTime)
        assertEquals(mapOf("h1" to "v1"), datum.headers)
    }
}
