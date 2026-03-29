package io.numaproj.numaflowkt.mapper.internal

import io.numaproj.numaflowkt.mapper.Mapper as KotlinMapper
import io.numaproj.numaflow.mapper.Mapper as JavaMapper
import io.numaproj.numaflow.mapper.Datum as JavaDatum
import io.numaproj.numaflow.mapper.MessageList as JavaMessageList
import kotlinx.coroutines.runBlocking

/**
 * Bridges the Kotlin [KotlinMapper] (suspend, List-based) to the
 * Java SDK's [JavaMapper] (blocking, MessageList-based).
 *
 * The Java SDK's Akka actor system calls [processMessage] on dedicated worker
 * threads. We use plain [runBlocking] (no custom dispatcher) to enter the
 * coroutine world -- the Akka threads are already dedicated workers, so there
 * is no benefit in shifting to `Dispatchers.Default`.
 *
 * **Error propagation:** Exceptions thrown by the Kotlin mapper propagate
 * naturally through [runBlocking] to the Akka supervisor, which handles
 * error logging and stream termination. No try-catch is needed here.
 *
 * **Null safety at the Java boundary:** The `keys` parameter comes from the
 * Java SDK which does not annotate nullability. We defensively coalesce
 * `keys?.toList()` even though it is declared non-null in Java.
 */
internal class MapperAdapter(private val mapper: KotlinMapper) : JavaMapper() {
    override fun processMessage(keys: Array<String>, datum: JavaDatum): JavaMessageList {
        return runBlocking {
            @Suppress("UNNECESSARY_SAFE_CALL")
            val kotlinKeys = keys?.toList() ?: emptyList()
            val kotlinDatum = datum.toKotlinDatum()
            val messages = mapper.processMessage(kotlinKeys, kotlinDatum)
            messages.toJavaMessageList()
        }
    }
}
