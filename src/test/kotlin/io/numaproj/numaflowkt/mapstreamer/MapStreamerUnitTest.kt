package io.numaproj.numaflowkt.mapstreamer

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class MapStreamerUnitTest {

    @Test
    fun `single message emission`() = runTest {
        val streamer = MapStreamer { keys, datum ->
            flow {
                emit(Message(value = datum.value, keys = keys))
            }
        }

        val flow = streamer.processMessage(listOf("k1"), Datum(value = "hello".toByteArray()))
        val results = flow.toList()

        assertEquals(1, results.size)
        assertEquals(Message(value = "hello".toByteArray(), keys = listOf("k1")), results[0])
    }

    @Test
    fun `multiple message emission`() = runTest {
        val streamer = MapStreamer { keys, datum ->
            flow {
                String(datum.value).split(" ").forEach { word ->
                    emit(Message(value = word.toByteArray(), keys = keys))
                }
            }
        }

        val flow = streamer.processMessage(listOf("k"), Datum(value = "hello world foo".toByteArray()))
        val results = flow.toList()

        assertEquals(3, results.size)
        assertEquals("hello", String(results[0].value))
        assertEquals("world", String(results[1].value))
        assertEquals("foo", String(results[2].value))
    }

    @Test
    fun `empty flow`() = runTest {
        val streamer = MapStreamer { _, _ ->
            flow { }
        }

        val flow = streamer.processMessage(emptyList(), Datum(value = "test".toByteArray()))
        val results = flow.toList()

        assertEquals(0, results.size)
    }

    @Test
    fun `async operations in flow`() = runTest {
        val streamer = MapStreamer { keys, datum ->
            flow {
                emit(Message(value = "start".toByteArray(), keys = keys))
                delay(10) // simulate async work
                emit(Message(value = "end".toByteArray(), keys = keys))
            }
        }

        val flow = streamer.processMessage(listOf("k"), Datum(value = "test".toByteArray()))
        val results = flow.toList()

        assertEquals(2, results.size)
        assertEquals("start", String(results[0].value))
        assertEquals("end", String(results[1].value))
    }

    @Test
    fun `keys and tags preserved`() = runTest {
        val streamer = MapStreamer { keys, datum ->
            flow {
                emit(Message(value = datum.value, keys = keys, tags = listOf("tag1")))
            }
        }

        val flow = streamer.processMessage(listOf("k1", "k2"), Datum(value = "test".toByteArray()))
        val results = flow.toList()

        assertEquals(listOf("k1", "k2"), results[0].keys)
        assertEquals(listOf("tag1"), results[0].tags)
    }
}
