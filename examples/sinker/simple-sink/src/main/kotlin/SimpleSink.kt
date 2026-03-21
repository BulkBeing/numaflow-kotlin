import io.numaproj.numaflowkt.sinker.*
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList

fun main() {
    sinkServer {
        sinker(Sinker { messages ->
            messages.map { datum ->
                println("Received: ${String(datum.value)}")
                Response.Ok(datum.id)
            }.toList()
        })
    }.run()
}
