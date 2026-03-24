import io.numaproj.numaflowkt.sinker.Response
import io.numaproj.numaflowkt.sinker.sinkServer
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList

fun main() {
    sinkServer {
        sinker { messages ->
            messages.map { datum ->
                println("Received: ${String(datum.value)}")
                Response.Ok(datum.id)
            }.toList()
        }
    }.run()
}
