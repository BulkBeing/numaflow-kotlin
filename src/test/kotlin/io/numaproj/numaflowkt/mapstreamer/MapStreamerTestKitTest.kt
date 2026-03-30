package io.numaproj.numaflowkt.mapstreamer

import kotlinx.coroutines.flow.flow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Instant

class MapStreamerTestKitTest {

    private val now = Instant.now()

    @Test
    fun `single output message over gRPC`() {
        val streamer = MapStreamer { keys, datum ->
            flow {
                emit(Message(value = String(datum.value).uppercase().toByteArray(), keys = keys))
            }
        }

        MapStreamerTestKit(streamer, MapStreamerConfig(port = 50081)).use { kit ->
            kit.start()
            val results = kit.sendRequest(
                keys = listOf("key1"),
                datum = Datum(value = "hello".toByteArray(), eventTime = now, watermark = now)
            )
            assertEquals(1, results.size)
            assertEquals("HELLO", String(results[0].value))
            assertEquals(listOf("key1"), results[0].keys)
        }
    }

    @Test
    fun `multiple streamed output messages over gRPC`() {
        val streamer = MapStreamer { keys, datum ->
            flow {
                String(datum.value).split(" ").forEach { word ->
                    emit(Message(value = word.toByteArray(), keys = keys))
                }
            }
        }

        MapStreamerTestKit(streamer, MapStreamerConfig(port = 50082)).use { kit ->
            kit.start()
            val results = kit.sendRequest(
                keys = listOf("k"),
                datum = Datum(value = "hello world".toByteArray(), eventTime = now, watermark = now)
            )
            assertEquals(2, results.size)
            assertEquals("hello", String(results[0].value))
            assertEquals("world", String(results[1].value))
        }
    }

    @Test
    fun `drop message over gRPC`() {
        val streamer = MapStreamer { _, _ ->
            flow {
                emit(Message.drop())
            }
        }

        MapStreamerTestKit(streamer, MapStreamerConfig(port = 50084)).use { kit ->
            kit.start()
            val results = kit.sendRequest(
                datum = Datum(value = "test".toByteArray(), eventTime = now, watermark = now)
            )
            assertEquals(1, results.size)
            assertEquals(listOf("U+005C__DROP__"), results[0].tags)
        }
    }
}
