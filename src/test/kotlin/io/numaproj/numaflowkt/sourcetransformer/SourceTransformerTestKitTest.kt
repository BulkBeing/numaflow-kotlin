package io.numaproj.numaflowkt.sourcetransformer

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Instant

class SourceTransformerTestKitTest {

    private val now = Instant.now()

    @Test
    fun `passthrough over gRPC`() {
        val transformer = SourceTransformer { keys, datum ->
            listOf(Message(
                value = datum.value,
                eventTime = datum.eventTime,
                keys = keys
            ))
        }

        SourceTransformerTestKit(transformer, SourceTransformerConfig(port = 50101)).use { kit ->
            kit.start()
            val results = kit.sendRequest(
                keys = listOf("key1"),
                datum = Datum(value = "hello".toByteArray(), eventTime = now, watermark = now)
            )
            assertEquals(1, results.size)
            assertEquals("hello", String(results[0].value))
            assertEquals(listOf("key1"), results[0].keys)
        }
    }

    @Test
    fun `event time reassignment over gRPC`() {
        val newTime = Instant.parse("2024-06-15T12:00:00Z")
        val transformer = SourceTransformer { keys, datum ->
            listOf(Message(
                value = datum.value,
                eventTime = newTime,
                keys = keys
            ))
        }

        SourceTransformerTestKit(transformer, SourceTransformerConfig(port = 50102)).use { kit ->
            kit.start()
            val results = kit.sendRequest(
                keys = listOf("k"),
                datum = Datum(value = "test".toByteArray(), eventTime = now, watermark = now)
            )
            assertEquals(1, results.size)
            assertEquals(newTime, results[0].eventTime)
        }
    }

    @Test
    fun `drop over gRPC`() {
        val transformer = SourceTransformer { _, datum ->
            listOf(Message.drop(datum.eventTime))
        }

        SourceTransformerTestKit(transformer, SourceTransformerConfig(port = 50103)).use { kit ->
            kit.start()
            val results = kit.sendRequest(
                datum = Datum(value = "test".toByteArray(), eventTime = now, watermark = now)
            )
            assertEquals(1, results.size)
            assertEquals(listOf("U+005C__DROP__"), results[0].tags)
        }
    }
}
