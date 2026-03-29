import io.numaproj.numaflowkt.mapper.Message
import io.numaproj.numaflowkt.mapper.mapServer

/**
 * A simple 1:1 mapper that converts each message payload to uppercase.
 *
 * Each input message produces exactly one output message with the same keys
 * and the payload transformed to uppercase. This demonstrates the minimal
 * wiring needed for a Numaflow Kotlin mapper UDF:
 *
 * 1. Define a mapper lambda inside [mapServer]
 * 2. Return a list of [Message]s (one per output)
 * 3. Call `run()` to start the gRPC server
 *
 * To run locally: `./gradlew :examples-mapper-simple-map:run`
 */
fun main() {
    mapServer {
        mapper { keys, datum ->
            val upper = String(datum.value).uppercase()
            listOf(Message(value = upper.toByteArray(), keys = keys))
        }
    }.run()
}
