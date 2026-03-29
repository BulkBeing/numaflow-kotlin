package io.numaproj.numaflowkt.mapper

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class MapperTestKitTest {

    @Test
    fun `simple mapping over gRPC`() {
        val mapper = Mapper { keys, datum ->
            val upper = String(datum.value).uppercase()
            listOf(Message(value = upper.toByteArray(), keys = keys))
        }

        MapperTestKit(mapper, MapperConfig(port = 50071)).use { kit ->
            kit.start()
            val results = kit.sendRequest(
                keys = listOf("key1"),
                datum = Datum(value = "hello".toByteArray())
            )
            assertEquals(1, results.size)
            assertEquals("HELLO", String(results[0].value))
            assertEquals(listOf("key1"), results[0].keys)
        }
    }

    @Test
    fun `flatmap over gRPC`() {
        val mapper = Mapper { keys, datum ->
            String(datum.value).split(",").map { word ->
                Message(value = word.trim().toByteArray(), keys = keys)
            }
        }

        MapperTestKit(mapper, MapperConfig(port = 50072)).use { kit ->
            kit.start()
            val results = kit.sendRequest(
                keys = listOf("k"),
                datum = Datum(value = "a,b,c".toByteArray())
            )
            assertEquals(3, results.size)
            assertEquals("a", String(results[0].value))
            assertEquals("b", String(results[1].value))
            assertEquals("c", String(results[2].value))
        }
    }

    @Test
    fun `drop over gRPC`() {
        val mapper = Mapper { _, _ ->
            listOf(Message.drop())
        }

        MapperTestKit(mapper, MapperConfig(port = 50073)).use { kit ->
            kit.start()
            val results = kit.sendRequest(
                datum = Datum(value = "test".toByteArray())
            )
            assertEquals(1, results.size)
            assertEquals(listOf("U+005C__DROP__"), results[0].tags)
        }
    }
}
