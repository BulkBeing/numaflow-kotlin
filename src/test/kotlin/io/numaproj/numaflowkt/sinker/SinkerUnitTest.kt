package io.numaproj.numaflowkt.sinker

import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class SinkerUnitTest {

    @Test
    fun `simple sink returns Ok for each message`() = runTest {
        val sink = Sinker { messages ->
            messages.map { Response.Ok(it.id) }.toList()
        }

        val result = sink.processMessages(
            flowOf(
                Datum(id = "1", value = "hello".toByteArray()),
                Datum(id = "2", value = "world".toByteArray())
            )
        )

        assertEquals(listOf(Response.Ok("1"), Response.Ok("2")), result)
    }

    @Test
    fun `sink handles failure for empty payload`() = runTest {
        val sink = Sinker { messages ->
            messages.map { datum ->
                if (datum.value.isEmpty()) Response.Failure(datum.id, "empty payload")
                else Response.Ok(datum.id)
            }.toList()
        }

        val result = sink.processMessages(
            flowOf(
                Datum(id = "1", value = byteArrayOf()),
                Datum(id = "2", value = "ok".toByteArray())
            )
        )

        assertEquals(Response.Failure("1", "empty payload"), result[0])
        assertEquals(Response.Ok("2"), result[1])
    }

    @Test
    fun `sink returns mixed response types`() = runTest {
        val sink = Sinker { messages ->
            messages.map { datum ->
                when (String(datum.value)) {
                    "ok" -> Response.Ok(datum.id)
                    "fail" -> Response.Failure(datum.id, "bad input")
                    "fallback" -> Response.Fallback(datum.id)
                    "serve" -> Response.Serve(datum.id, "served".toByteArray())
                    "forward" -> Response.OnSuccess(datum.id, Message(value = "forwarded".toByteArray()))
                    else -> Response.Failure(datum.id, "unknown")
                }
            }.toList()
        }

        val result = sink.processMessages(
            flowOf(
                Datum(id = "1", value = "ok".toByteArray()),
                Datum(id = "2", value = "fail".toByteArray()),
                Datum(id = "3", value = "fallback".toByteArray()),
                Datum(id = "4", value = "serve".toByteArray()),
                Datum(id = "5", value = "forward".toByteArray()),
            )
        )

        assertEquals(5, result.size)
        assertEquals(Response.Ok("1"), result[0])
        assertEquals(Response.Failure("2", "bad input"), result[1])
        assertEquals(Response.Fallback("3"), result[2])
        assertEquals(Response.Serve("4", "served".toByteArray()), result[3])
        assertEquals(
            Response.OnSuccess("5", Message(value = "forwarded".toByteArray())),
            result[4]
        )
    }

    @Test
    fun `batch sink collects all then processes`() = runTest {
        val sink = Sinker { messages ->
            val datums = messages.toList()
            datums.map { Response.Ok(it.id) }
        }

        val result = sink.processMessages(
            flowOf(
                Datum(id = "a", value = "1".toByteArray()),
                Datum(id = "b", value = "2".toByteArray()),
                Datum(id = "c", value = "3".toByteArray()),
            )
        )

        assertEquals(3, result.size)
        assertEquals(listOf("a", "b", "c"), result.map { it.id })
    }

    @Test
    fun `empty flow produces empty result`() = runTest {
        val sink = Sinker { messages ->
            messages.map { Response.Ok(it.id) }.toList()
        }

        val result = sink.processMessages(flowOf())
        assertEquals(emptyList<Response>(), result)
    }
}
