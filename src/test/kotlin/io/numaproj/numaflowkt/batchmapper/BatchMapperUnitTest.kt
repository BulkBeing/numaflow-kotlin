package io.numaproj.numaflowkt.batchmapper

import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Instant

class BatchMapperUnitTest {

    private val now = Instant.now()

    @Test
    fun `simple 1-1 batch mapping`() = runTest {
        val batchMapper = BatchMapper { messages ->
            messages.map { datum ->
                BatchResponse(
                    id = datum.id,
                    messages = listOf(Message(value = String(datum.value).uppercase().toByteArray(), keys = datum.keys))
                )
            }.toList()
        }

        val result = batchMapper.processMessage(
            flowOf(
                Datum(id = "1", value = "hello".toByteArray(), keys = listOf("k1"), eventTime = now, watermark = now),
                Datum(id = "2", value = "world".toByteArray(), keys = listOf("k2"), eventTime = now, watermark = now)
            )
        )

        assertEquals(2, result.size)
        assertEquals("1", result[0].id)
        assertEquals("HELLO", String(result[0].messages[0].value))
        assertEquals("2", result[1].id)
        assertEquals("WORLD", String(result[1].messages[0].value))
    }

    @Test
    fun `flatmap in batch`() = runTest {
        val batchMapper = BatchMapper { messages ->
            messages.map { datum ->
                BatchResponse(
                    id = datum.id,
                    messages = String(datum.value).split(",").map { word ->
                        Message(value = word.trim().toByteArray(), keys = datum.keys)
                    }
                )
            }.toList()
        }

        val result = batchMapper.processMessage(
            flowOf(Datum(id = "1", value = "a,b,c".toByteArray(), eventTime = now, watermark = now))
        )

        assertEquals(1, result.size)
        assertEquals(3, result[0].messages.size)
    }

    @Test
    fun `mixed output sizes`() = runTest {
        val batchMapper = BatchMapper { messages ->
            messages.map { datum ->
                val count = String(datum.value).toInt()
                BatchResponse(
                    id = datum.id,
                    messages = (1..count).map { i ->
                        Message(value = "msg-$i".toByteArray())
                    }
                )
            }.toList()
        }

        val result = batchMapper.processMessage(
            flowOf(
                Datum(id = "1", value = "1".toByteArray(), eventTime = now, watermark = now),
                Datum(id = "2", value = "3".toByteArray(), eventTime = now, watermark = now)
            )
        )

        assertEquals(1, result[0].messages.size)
        assertEquals(3, result[1].messages.size)
    }

    @Test
    fun `batch with drop`() = runTest {
        val batchMapper = BatchMapper { messages ->
            messages.map { datum ->
                BatchResponse(id = datum.id, messages = listOf(Message.drop()))
            }.toList()
        }

        val result = batchMapper.processMessage(
            flowOf(Datum(id = "1", value = "test".toByteArray(), eventTime = now, watermark = now))
        )

        assertEquals(Message.drop(), result[0].messages[0])
    }

    @Test
    fun `empty flow`() = runTest {
        val batchMapper = BatchMapper { messages ->
            messages.map { datum ->
                BatchResponse(id = datum.id, messages = listOf(Message(value = datum.value)))
            }.toList()
        }

        val result = batchMapper.processMessage(flowOf())
        assertEquals(emptyList<BatchResponse>(), result)
    }

    @Test
    fun `collect then process`() = runTest {
        val batchMapper = BatchMapper { messages ->
            val datums = messages.toList()
            datums.map { datum ->
                BatchResponse(
                    id = datum.id,
                    messages = listOf(Message(value = datum.value, keys = datum.keys))
                )
            }
        }

        val result = batchMapper.processMessage(
            flowOf(
                Datum(id = "a", value = "1".toByteArray(), eventTime = now, watermark = now),
                Datum(id = "b", value = "2".toByteArray(), eventTime = now, watermark = now),
                Datum(id = "c", value = "3".toByteArray(), eventTime = now, watermark = now)
            )
        )

        assertEquals(3, result.size)
        assertEquals(listOf("a", "b", "c"), result.map { it.id })
    }
}
