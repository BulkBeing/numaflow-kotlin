package io.numaproj.numaflowkt.mapper

import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class MapperUnitTest {

    @Test
    fun `simple 1-1 mapping`() = runTest {
        val mapper = Mapper { keys, datum ->
            val upper = String(datum.value).uppercase()
            listOf(Message(value = upper.toByteArray(), keys = keys))
        }

        val result = mapper.processMessage(
            listOf("key1"),
            Datum(value = "hello".toByteArray())
        )

        assertEquals(1, result.size)
        assertEquals(Message(value = "HELLO".toByteArray(), keys = listOf("key1")), result[0])
    }

    @Test
    fun `flatmap 1-N`() = runTest {
        val mapper = Mapper { keys, datum ->
            String(datum.value).split(",").map { word ->
                Message(value = word.trim().toByteArray(), keys = keys)
            }
        }

        val result = mapper.processMessage(
            listOf("k"),
            Datum(value = "a,b,c".toByteArray())
        )

        assertEquals(3, result.size)
        assertEquals("a", String(result[0].value))
        assertEquals("b", String(result[1].value))
        assertEquals("c", String(result[2].value))
    }

    @Test
    fun `filter with drop`() = runTest {
        val mapper = Mapper { keys, datum ->
            if (String(datum.value).startsWith("keep"))
                listOf(Message(value = datum.value, keys = keys))
            else
                listOf(Message.drop())
        }

        val kept = mapper.processMessage(listOf("k"), Datum(value = "keep-this".toByteArray()))
        val dropped = mapper.processMessage(listOf("k"), Datum(value = "discard".toByteArray()))

        assertEquals(1, kept.size)
        assertEquals("keep-this", String(kept[0].value))
        assertEquals(1, dropped.size)
        assertEquals(Message.drop(), dropped[0])
    }

    @Test
    fun `preserve keys`() = runTest {
        val mapper = Mapper { keys, datum ->
            listOf(Message(value = datum.value, keys = keys))
        }

        val result = mapper.processMessage(
            listOf("key1", "key2"),
            Datum(value = "test".toByteArray())
        )

        assertEquals(listOf("key1", "key2"), result[0].keys)
    }

    @Test
    fun `empty input value`() = runTest {
        val mapper = Mapper { keys, datum ->
            listOf(Message(value = datum.value, keys = keys))
        }

        val result = mapper.processMessage(emptyList(), Datum(value = byteArrayOf()))

        assertEquals(1, result.size)
        assertEquals(0, result[0].value.size)
    }

    @Test
    fun `headers preserved`() = runTest {
        val mapper = Mapper { keys, datum ->
            val headerValue = datum.headers["source"] ?: "unknown"
            listOf(Message(value = headerValue.toByteArray(), keys = keys))
        }

        val result = mapper.processMessage(
            emptyList(),
            Datum(
                value = "test".toByteArray(),
                headers = mapOf("source" to "kafka")
            )
        )

        assertEquals("kafka", String(result[0].value))
    }
}
