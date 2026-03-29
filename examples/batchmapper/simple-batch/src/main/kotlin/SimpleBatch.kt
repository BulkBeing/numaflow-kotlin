import io.numaproj.numaflowkt.batchmapper.BatchResponse
import io.numaproj.numaflowkt.batchmapper.Message
import io.numaproj.numaflowkt.batchmapper.batchMapServer
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList

/**
 * A simple batch mapper that converts each message payload to uppercase.
 *
 * Processes the batch by streaming through the input [Flow][kotlinx.coroutines.flow.Flow]
 * and producing one [BatchResponse] per input [Datum][io.numaproj.numaflowkt.batchmapper.Datum],
 * correlated by `datum.id`. Each response contains a single output [Message] with the
 * uppercased payload.
 *
 * This demonstrates the simplest BatchMapper pattern: stream-process each datum
 * individually. For bulk optimizations (e.g. batch DB lookups), collect the flow
 * first with `messages.toList()`, then process in bulk.
 *
 * To run locally: `./gradlew :examples-batchmapper-simple-batch:run`
 */
fun main() {
    batchMapServer {
        batchMapper { messages ->
            messages.map { datum ->
                BatchResponse(
                    id = datum.id,
                    messages = listOf(
                        Message(
                            value = String(datum.value).uppercase().toByteArray(),
                            keys = datum.keys
                        )
                    )
                )
            }.toList()
        }
    }.run()
}
