import io.numaproj.numaflowkt.mapstreamer.Message
import io.numaproj.numaflowkt.mapstreamer.mapStreamServer
import kotlinx.coroutines.flow.flow

/**
 * A streaming word splitter that emits each word as a separate message.
 *
 * For an input like `"hello world foo"`, this streamer emits three messages
 * incrementally: `"hello"`, `"world"`, `"foo"`. Unlike a regular mapper which
 * collects all results before returning, the MapStreamer sends each word to the
 * downstream as soon as it is emitted from the [flow], reducing latency for
 * downstream consumers.
 *
 * This demonstrates the MapStreamer pattern: return a [flow] that emits [Message]s
 * incrementally. The SDK collects the flow and forwards each message eagerly.
 *
 * To run locally: `./gradlew :examples-mapstreamer-word-splitter:run`
 */
fun main() {
    mapStreamServer {
        mapStreamer { keys, datum ->
            flow {
                String(datum.value).split(" ").forEach { word ->
                    emit(Message(value = word.toByteArray(), keys = keys))
                }
            }
        }
    }.run()
}
