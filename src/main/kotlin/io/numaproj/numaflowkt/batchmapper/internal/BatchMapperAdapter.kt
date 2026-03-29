package io.numaproj.numaflowkt.batchmapper.internal

import io.numaproj.numaflowkt.batchmapper.BatchMapper as KotlinBatchMapper
import io.numaproj.numaflow.batchmapper.BatchMapper as JavaBatchMapper
import io.numaproj.numaflow.batchmapper.DatumIterator as JavaDatumIterator
import io.numaproj.numaflow.batchmapper.BatchResponses as JavaBatchResponses
import kotlinx.coroutines.runBlocking

/**
 * Bridges the Kotlin [KotlinBatchMapper] (suspend, Flow-based) to the
 * Java SDK's [JavaBatchMapper] (blocking, DatumIterator-based).
 *
 * The Java SDK runs `processMessage` on an `ExecutorService` thread pool
 * (`availableProcessors * 2` threads), unlike Mapper/MapStreamer which use Akka
 * actors. We use plain [runBlocking] (no custom dispatcher) to enter the coroutine
 * world -- the ExecutorService threads are already dedicated workers.
 *
 * **Flow conversion:** The Java SDK's [JavaDatumIterator] is a blocking iterator
 * that returns `null` at EOF. We convert it to a Kotlin `Flow<Datum>` via
 * [asFlow][io.numaproj.numaflowkt.batchmapper.internal.asFlow] so the user
 * receives idiomatic streaming input.
 *
 * **Error propagation:** Exceptions from the Kotlin BatchMapper propagate naturally
 * through [runBlocking] to the ExecutorService, which handles error logging.
 * `InterruptedException` (only occurs during shutdown) is also allowed to propagate,
 * preserving the user's ability to catch it for cleanup (close DB, flush buffers).
 */
internal class BatchMapperAdapter(private val batchMapper: KotlinBatchMapper) : JavaBatchMapper() {
    override fun processMessage(datumStream: JavaDatumIterator): JavaBatchResponses {
        return runBlocking {
            val flow = datumStream.asFlow()
            val responses = batchMapper.processMessage(flow)
            responses.toJavaBatchResponses()
        }
    }
}
