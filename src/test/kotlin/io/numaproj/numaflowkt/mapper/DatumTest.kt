package io.numaproj.numaflowkt.mapper

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Instant

class DatumTest {

    private val now = Instant.now()

    @Test
    fun `datum equality compares value by content`() {
        val d1 = Datum(value = "hello".toByteArray(), eventTime = now, watermark = now)
        val d2 = Datum(value = "hello".toByteArray(), eventTime = now, watermark = now)
        assertEquals(d1, d2)
        assertEquals(d1.hashCode(), d2.hashCode())
    }

    @Test
    fun `datums with different values are not equal`() {
        val d1 = Datum(value = "hello".toByteArray(), eventTime = now, watermark = now)
        val d2 = Datum(value = "world".toByteArray(), eventTime = now, watermark = now)
        assertNotEquals(d1, d2)
    }

    @Test
    fun `datum defaults are empty headers`() {
        val datum = Datum(value = byteArrayOf(), eventTime = now, watermark = now)
        assertEquals(emptyMap<String, String>(), datum.headers)
    }

    @Test
    fun `datum with all fields populated`() {
        val datum = Datum(
            value = "payload".toByteArray(),
            eventTime = now,
            watermark = now,
            headers = mapOf("h1" to "v1")
        )
        assertArrayEquals("payload".toByteArray(), datum.value)
        assertEquals(now, datum.eventTime)
        assertEquals(now, datum.watermark)
        assertEquals(mapOf("h1" to "v1"), datum.headers)
    }
}
