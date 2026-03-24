import io.numaproj.numaflowkt.sinker.*
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlin.random.Random

/**
 * Demonstrates OnSuccess and Fallback response routing.
 *
 * Ported from the Java SDK's OnSuccess example. Simulates writing to a
 * primary sink — on success, forwards a custom message to the on-success
 * sink; on failure, routes to the fallback sink.
 */
val onSuccessSink = Sinker { messages ->
    messages.map { datum ->
        try {
            val msg = String(datum.value)
            println("Received message: $msg, id: ${datum.id}, headers: ${datum.headers}")
            if (writeToPrimarySink()) {
                println("Writing to onSuccess sink: ${datum.id}")
                Response.OnSuccess(
                    datum.id,
                    Message(value = "Successfully wrote message with ID: ${datum.id}".toByteArray())
                )
            } else {
                println("Writing to fallback sink: ${datum.id}")
                Response.Fallback(datum.id)
            }
        } catch (e: Exception) {
            println("Error while writing to any sink: ${e.message}")
            Response.Failure(datum.id, e.message ?: "unknown error")
        }
    }.toList()
}

/** Simulates write success/failure to primary sink. */
fun writeToPrimarySink(): Boolean = Random.nextBoolean()

fun main() {
    sinkServer {
        sinker(onSuccessSink)
    }.run()
}
