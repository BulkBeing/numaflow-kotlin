package io.numaproj.numaflowkt.sinker

import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Instant

class SinkerTestKitTest {

    private val now = Instant.now()

    @Test
    fun `integration test with real gRPC - Ok responses`() {
        val sink = Sinker { messages ->
            messages.map { datum ->
                Response.Ok(datum.id)
            }.toList()
        }

        SinkerTestKit(sink, SinkerConfig(port = 50061)).use { kit ->
            kit.start()
            val results = kit.sendMessages(
                Datum(id = "1", value = "hello".toByteArray(), eventTime = now, watermark = now),
                Datum(id = "2", value = "world".toByteArray(), eventTime = now, watermark = now)
            )
            assertEquals(2, results.size)
            assertEquals(Response.Ok("1"), results[0])
            assertEquals(Response.Ok("2"), results[1])
        }
    }

    @Test
    fun `integration test with failure responses`() {
        val sink = Sinker { messages ->
            messages.map { datum ->
                if (datum.value.isEmpty()) Response.Failure(datum.id, "empty")
                else Response.Ok(datum.id)
            }.toList()
        }

        SinkerTestKit(sink, SinkerConfig(port = 50062)).use { kit ->
            kit.start()
            val results = kit.sendMessages(
                Datum(id = "1", value = byteArrayOf(), eventTime = now, watermark = now),
                Datum(id = "2", value = "ok".toByteArray(), eventTime = now, watermark = now)
            )
            assertEquals(Response.Failure("1", "empty"), results[0])
            assertEquals(Response.Ok("2"), results[1])
        }
    }

    @Test
    fun `integration test with fallback responses`() {
        val sink = Sinker { messages ->
            messages.map { datum -> Response.Fallback(datum.id) }.toList()
        }

        SinkerTestKit(sink, SinkerConfig(port = 50063)).use { kit ->
            kit.start()
            val results = kit.sendMessages(
                Datum(id = "1", value = "test".toByteArray(), eventTime = now, watermark = now)
            )
            assertEquals(1, results.size)
            assertEquals(Response.Fallback("1"), results[0])
        }
    }
}
