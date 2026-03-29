package io.numaproj.numaflowkt.batchmapper

import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class BatchMapperTestKitTest {

    @Test
    fun `simple batch mapping over gRPC`() {
        val batchMapper = BatchMapper { messages ->
            messages.map { datum ->
                BatchResponse(
                    id = datum.id,
                    messages = listOf(Message(
                        value = String(datum.value).uppercase().toByteArray(),
                        keys = datum.keys
                    ))
                )
            }.toList()
        }

        BatchMapperTestKit(batchMapper, BatchMapperConfig(port = 50091)).use { kit ->
            kit.start()
            val results = kit.sendBatch(
                Datum(id = "1", value = "hello".toByteArray(), keys = listOf("k1")),
                Datum(id = "2", value = "world".toByteArray(), keys = listOf("k2"))
            )
            assertEquals(2, results.size)
            assertEquals("HELLO", String(results["1"]!![0].value))
            assertEquals("WORLD", String(results["2"]!![0].value))
        }
    }

    @Test
    fun `batch with multiple outputs per input over gRPC`() {
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

        BatchMapperTestKit(batchMapper, BatchMapperConfig(port = 50092)).use { kit ->
            kit.start()
            val results = kit.sendBatch(
                Datum(id = "1", value = "a,b".toByteArray())
            )
            assertEquals(2, results["1"]!!.size)
            assertEquals("a", String(results["1"]!![0].value))
            assertEquals("b", String(results["1"]!![1].value))
        }
    }

    @Test
    fun `batch with drop messages over gRPC`() {
        val batchMapper = BatchMapper { messages ->
            messages.map { datum ->
                BatchResponse(id = datum.id, messages = listOf(Message.drop()))
            }.toList()
        }

        BatchMapperTestKit(batchMapper, BatchMapperConfig(port = 50093)).use { kit ->
            kit.start()
            val results = kit.sendBatch(
                Datum(id = "1", value = "test".toByteArray())
            )
            assertEquals(1, results.size)
            assertEquals(listOf("U+005C__DROP__"), results["1"]!![0].tags)
        }
    }
}
