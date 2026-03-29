import io.numaproj.numaflowkt.mapper.Message
import io.numaproj.numaflowkt.mapper.mapServer

/**
 * A 1:N flatmap mapper that splits comma-separated input into individual messages.
 *
 * For an input like `"apple,banana,cherry"`, this mapper produces three output
 * messages: `"apple"`, `"banana"`, `"cherry"`, each with the original keys.
 * This demonstrates the flatmap pattern where a single input message fans out
 * to multiple output messages.
 *
 * To run locally: `./gradlew :examples-mapper-flatmap:run`
 */
fun main() {
    mapServer {
        mapper { keys, datum ->
            String(datum.value).split(",").map { word ->
                Message(value = word.trim().toByteArray(), keys = keys)
            }
        }
    }.run()
}
