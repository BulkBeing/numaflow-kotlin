import io.numaproj.numaflowkt.sourcetransformer.Message
import io.numaproj.numaflowkt.sourcetransformer.sourceTransformerServer
import java.time.Instant

/**
 * A source transformer that filters messages by event time and normalizes
 * event times to canonical bucket boundaries.
 *
 * - Messages before 2022 are dropped (but still counted for watermark).
 * - Messages within 2022 get event time normalized to Jan 1, 2022.
 * - Messages after 2022 get event time normalized to Jan 1, 2023.
 *
 * This demonstrates the primary use case of source transformers: filtering
 * and reassigning event times at the source vertex for watermark progression.
 *
 * Ported from the Java SDK's EventTimeFilterFunction example.
 *
 * To run locally: `./gradlew :examples-sourcetransformer-event-time-filter:run`
 */
fun main() {
    sourceTransformerServer {
        sourceTransformer { keys, datum ->
            val eventTime = datum.eventTime
            val cutoff = Instant.parse("2022-01-01T00:00:00Z")
            val bucket = Instant.parse("2023-01-01T00:00:00Z")

            when {
                eventTime.isBefore(cutoff) ->
                    listOf(Message.drop(eventTime))
                eventTime.isBefore(bucket) ->
                    listOf(Message(
                        value = datum.value,
                        eventTime = cutoff,
                        keys = keys,
                        tags = listOf("within_2022")
                    ))
                else ->
                    listOf(Message(
                        value = datum.value,
                        eventTime = bucket,
                        keys = keys,
                        tags = listOf("after_2022")
                    ))
            }
        }
    }.run()
}
