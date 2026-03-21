import io.numaproj.numaflowkt.sinker.*
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.toList

/**
 * Demonstrates concurrent processing of messages using Kotlin coroutines.
 *
 * Collects all messages in the batch first, then processes each one
 * concurrently via [async]. This pattern is useful when the per-message
 * processing involves I/O (HTTP calls, database writes) and you want
 * to maximize throughput within a single batch.
 */
val concurrentSink = Sinker { messages ->
    val datums = messages.toList()
    coroutineScope {
        datums.map { datum ->
            async {
                try {
                    // Simulate async processing
                    println("Processing: ${datum.id}")
                    Response.Ok(datum.id)
                } catch (e: Exception) {
                    Response.Failure(datum.id, e.message ?: "error")
                }
            }
        }.awaitAll()
    }
}

fun main() {
    sinkServer {
        sinker(concurrentSink)
    }.run()
}
