package io.numaproj.numaflowkt.sinker.internal

import io.numaproj.numaflow.sinker.DatumIterator
import io.numaproj.numaflow.sinker.ResponseList
import io.numaproj.numaflow.sinker.Sinker as JavaSinker
import kotlinx.coroutines.runBlocking

/**
 * Adapts a Kotlin [io.numaproj.numaflowkt.sinker.Sinker] to a Java
 * [io.numaproj.numaflow.sinker.Sinker].
 *
 * The Java SDK calls [processMessages] on its sink thread pool
 * (a fixed pool of `Runtime.availableProcessors() * 2` threads).
 * [runBlocking] bridges from the Java thread into a coroutine so the
 * Kotlin sinker's `suspend fun` can execute. Users can freely launch
 * child coroutines from within their [processMessages] implementation.
 */
internal class SinkerAdapter(
    private val kotlinSinker: io.numaproj.numaflowkt.sinker.Sinker
) : JavaSinker() {

    override fun processMessages(datumIterator: DatumIterator): ResponseList {
        val flow = datumIterator.asFlow()
        val responses = runBlocking {
            kotlinSinker.processMessages(flow)
        }
        return responses.toJavaResponseList()
    }
}
