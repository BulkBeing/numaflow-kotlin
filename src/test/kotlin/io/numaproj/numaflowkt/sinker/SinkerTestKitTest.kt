package io.numaproj.numaflowkt.sinker

import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class SinkerTestKitTest {

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
                Datum(id = "1", value = "hello".toByteArray()),
                Datum(id = "2", value = "world".toByteArray())
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
                Datum(id = "1", value = byteArrayOf()),
                Datum(id = "2", value = "ok".toByteArray())
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
                Datum(id = "1", value = "test".toByteArray())
            )
            assertEquals(1, results.size)
            assertEquals(Response.Fallback("1"), results[0])
        }
    }
}
