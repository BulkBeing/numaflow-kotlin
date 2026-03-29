package io.numaproj.numaflowkt.mapstreamer.internal

import io.numaproj.numaflowkt.mapstreamer.MapStreamer as KotlinMapStreamer
import io.numaproj.numaflow.mapstreamer.MapStreamer as JavaMapStreamer
import io.numaproj.numaflow.mapstreamer.Datum as JavaDatum
import io.numaproj.numaflow.mapstreamer.OutputObserver as JavaOutputObserver
import kotlinx.coroutines.runBlocking

/**
 * Bridges the Kotlin [KotlinMapStreamer] (suspend, Flow-based) to the
 * Java SDK's [JavaMapStreamer] (blocking, OutputObserver callback-based).
 *
 * The Java SDK's Akka actor system calls [processMessage] on dedicated worker
 * threads. We use plain [runBlocking] (no custom dispatcher) to enter the
 * coroutine world -- the Akka threads are already dedicated workers.
 *
 * **Eager forwarding:** Each emitted message is sent to the [JavaOutputObserver]
 * immediately during flow collection. This preserves true streaming semantics --
 * downstream consumers see messages as soon as the user emits them, rather than
 * waiting for the entire flow to complete.
 *
 * **EOF handling:** The adapter does NOT call `outputObserver.sendEOF()`. The
 * Java SDK's `MapStreamerActor` automatically calls `sendEOF()` after
 * `processMessage()` returns, which signals end-of-transmission for this request.
 *
 * **Error propagation:** Exceptions from the Kotlin MapStreamer propagate naturally
 * through [runBlocking] to the Akka supervisor. No try-catch is needed here.
 *
 * **Null safety at the Java boundary:** The `keys` parameter comes from the
 * Java SDK which does not annotate nullability. We defensively coalesce
 * `keys?.toList()` even though it is declared non-null in Java.
 */
internal class MapStreamerAdapter(private val mapStreamer: KotlinMapStreamer) : JavaMapStreamer() {
    override fun processMessage(
        keys: Array<String>,
        datum: JavaDatum,
        outputObserver: JavaOutputObserver
    ) {
        runBlocking {
            @Suppress("UNNECESSARY_SAFE_CALL")
            val kotlinKeys = keys?.toList() ?: emptyList()
            val kotlinDatum = datum.toKotlinDatum()
            val flow = mapStreamer.processMessage(kotlinKeys, kotlinDatum)
            flow.collect { message ->
                outputObserver.send(message.toJavaMessage())
            }
        }
    }
}
