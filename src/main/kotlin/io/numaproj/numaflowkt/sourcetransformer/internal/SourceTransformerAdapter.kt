package io.numaproj.numaflowkt.sourcetransformer.internal

import io.numaproj.numaflowkt.sourcetransformer.SourceTransformer as KotlinSourceTransformer
import io.numaproj.numaflow.sourcetransformer.SourceTransformer as JavaSourceTransformer
import io.numaproj.numaflow.sourcetransformer.Datum as JavaDatum
import io.numaproj.numaflow.sourcetransformer.MessageList as JavaMessageList
import kotlinx.coroutines.runBlocking

/**
 * Bridges the Kotlin [KotlinSourceTransformer] (suspend, List-based) to the
 * Java SDK's [JavaSourceTransformer] (blocking, MessageList-based).
 *
 * The Java SDK's Akka actor system calls [processMessage] on dedicated worker
 * threads (TransformerActor). We use plain [runBlocking] (no custom dispatcher)
 * to enter the coroutine world — the Akka threads are already dedicated workers,
 * so there is no benefit in shifting to `Dispatchers.Default`. Adding a dispatcher
 * would just waste a thread hop while the Akka thread sits blocked waiting anyway.
 *
 * **Error propagation:** Exceptions thrown by the Kotlin transformer propagate
 * naturally through [runBlocking] to the Akka supervisor
 * (TransformSupervisorActor), which handles error logging and stream termination.
 * No try-catch is needed here.
 *
 * **Null safety at the Java boundary:** The `keys` parameter comes from the
 * Java SDK which does not annotate nullability. We defensively coalesce
 * `keys?.toList()` even though it is declared non-null in Java.
 */
internal class SourceTransformerAdapter(
    private val transformer: KotlinSourceTransformer
) : JavaSourceTransformer() {

    override fun processMessage(keys: Array<String>, datum: JavaDatum): JavaMessageList {
        return runBlocking {
            @Suppress("UNNECESSARY_SAFE_CALL")
            val kotlinKeys = keys?.toList() ?: emptyList()
            val kotlinDatum = datum.toKotlinDatum()
            val messages = transformer.processMessage(kotlinKeys, kotlinDatum)
            messages.toJavaMessageList()
        }
    }
}
