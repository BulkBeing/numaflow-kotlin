package io.numaproj.numaflowkt.sinker

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
    fun `datum with all fields populated`() {
        val now = Instant.now()
        val meta = UserMetadata()
        meta.put("g", "k", "v".toByteArray())

        val datum = Datum(
            id = "msg-1",
            value = "payload".toByteArray(),
            keys = listOf("key1", "key2"),
            eventTime = now,
            watermark = now,
            headers = mapOf("h1" to "v1"),
            userMetadata = meta
        )

        assertEquals("msg-1", datum.id)
        assertArrayEquals("payload".toByteArray(), datum.value)
        assertEquals(listOf("key1", "key2"), datum.keys)
        assertEquals(now, datum.eventTime)
        assertEquals(mapOf("h1" to "v1"), datum.headers)
    }

    @Test
    fun `datum defaults are null`() {
        val datum = Datum(id = "1", value = byteArrayOf())
        assertNull(datum.keys)
        assertNull(datum.eventTime)
        assertNull(datum.watermark)
        assertNull(datum.headers)
        assertNull(datum.userMetadata)
        assertNull(datum.systemMetadata)
    }
}
