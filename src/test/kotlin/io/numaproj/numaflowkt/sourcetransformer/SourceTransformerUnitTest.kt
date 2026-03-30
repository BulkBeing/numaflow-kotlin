package io.numaproj.numaflowkt.sourcetransformer

import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Instant

class SourceTransformerUnitTest {

    private val now = Instant.now()

    @Test
    fun `simple passthrough with event time`() = runTest {
        val transformer = SourceTransformer { keys, datum ->
            listOf(Message(
                value = datum.value,
                eventTime = datum.eventTime,
                keys = keys
            ))
        }

        val result = transformer.processMessage(
            listOf("key1"),
            Datum(value = "hello".toByteArray(), eventTime = now, watermark = now)
        )

        assertEquals(1, result.size)
        assertEquals("hello", String(result[0].value))
        assertEquals(now, result[0].eventTime)
        assertEquals(listOf("key1"), result[0].keys)
    }

    @Test
    fun `reassign event time`() = runTest {
        val newTime = Instant.parse("2024-01-01T00:00:00Z")
        val transformer = SourceTransformer { keys, datum ->
            listOf(Message(
                value = datum.value,
                eventTime = newTime,
                keys = keys
            ))
        }

        val result = transformer.processMessage(
            listOf("k"),
            Datum(value = "test".toByteArray(), eventTime = now, watermark = now)
        )

        assertEquals(newTime, result[0].eventTime)
    }

    @Test
    fun `flatmap with event time`() = runTest {
        val transformer = SourceTransformer { keys, datum ->
            String(datum.value).split(",").map { word ->
                Message(
                    value = word.trim().toByteArray(),
                    eventTime = datum.eventTime,
                    keys = keys
                )
            }
        }

        val result = transformer.processMessage(
            listOf("k"),
            Datum(value = "a,b,c".toByteArray(), eventTime = now, watermark = now)
        )

        assertEquals(3, result.size)
        assertEquals("a", String(result[0].value))
        assertEquals("b", String(result[1].value))
        assertEquals("c", String(result[2].value))
    }

    @Test
    fun `drop with event time`() = runTest {
        val transformer = SourceTransformer { _, datum ->
            listOf(Message.drop(datum.eventTime))
        }

        val result = transformer.processMessage(
            listOf("k"),
            Datum(value = "discard".toByteArray(), eventTime = now, watermark = now)
        )

        assertEquals(1, result.size)
        assertEquals(Message.drop(now), result[0])
    }

    @Test
    fun `preserve headers`() = runTest {
        val transformer = SourceTransformer { keys, datum ->
            val headerValue = datum.headers["source"] ?: "unknown"
            listOf(Message(
                value = headerValue.toByteArray(),
                eventTime = datum.eventTime,
                keys = keys
            ))
        }

        val result = transformer.processMessage(
            emptyList(),
            Datum(
                value = "test".toByteArray(),
                eventTime = now,
                watermark = now,
                headers = mapOf("source" to "kafka")
            )
        )

        assertEquals("kafka", String(result[0].value))
    }

    @Test
    fun `empty input value`() = runTest {
        val transformer = SourceTransformer { keys, datum ->
            listOf(Message(value = datum.value, eventTime = datum.eventTime, keys = keys))
        }

        val result = transformer.processMessage(
            emptyList(),
            Datum(value = byteArrayOf(), eventTime = now, watermark = now)
        )

        assertEquals(1, result.size)
        assertEquals(0, result[0].value.size)
    }
}
