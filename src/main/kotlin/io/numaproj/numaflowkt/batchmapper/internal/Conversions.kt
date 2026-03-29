package io.numaproj.numaflowkt.batchmapper.internal

import io.numaproj.numaflowkt.batchmapper.BatchResponse
import io.numaproj.numaflowkt.batchmapper.Datum
import io.numaproj.numaflowkt.batchmapper.Message
import io.numaproj.numaflow.batchmapper.Datum as JavaDatum
import io.numaproj.numaflow.batchmapper.DatumIterator as JavaDatumIterator
import io.numaproj.numaflow.batchmapper.Message as JavaMessage
import io.numaproj.numaflow.batchmapper.BatchResponse as JavaBatchResponse
import io.numaproj.numaflow.batchmapper.BatchResponses as JavaBatchResponses
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

/**
 * Convert Java SDK's blocking [JavaDatumIterator] to a Kotlin [Flow].
 *
 * The Java SDK's `DatumIterator.next()` blocks the calling thread until
 * the next element is available, and returns `null` when the batch boundary
 * (EOF_DATUM) is reached. We wrap this in a flow that emits each element
 * as a Kotlin [Datum] and completes when `null` is received.
 *
 * This is the same pattern used by the Sinker adapter for converting
 * the Java SDK's blocking iterators to Kotlin Flows.
 *
 * **Note:** Because `next()` is blocking, this flow should be collected on
 * a thread that can block (which is the case here -- the adapter runs inside
 * [kotlinx.coroutines.runBlocking] on an ExecutorService thread).
 */
internal fun JavaDatumIterator.asFlow(): Flow<Datum> = flow {
    while (true) {
        val datum = next() ?: break
        emit(datum.toKotlinDatum())
    }
}

/**
 * Convert Java SDK Datum to Kotlin Datum.
 *
 * Null-coalesces defensively: the Java SDK does not annotate nullability,
 * so `id`, `value`, `keys`, and `headers` may arrive as `null` despite
 * being logically non-null.
 */
internal fun JavaDatum.toKotlinDatum(): Datum = Datum(
    id = this.id ?: "",
    value = this.value ?: byteArrayOf(),
    keys = this.keys?.toList() ?: emptyList(),
    eventTime = this.eventTime,
    watermark = this.watermark,
    headers = this.headers ?: emptyMap()
)

/**
 * Convert a list of Kotlin [BatchResponse]s to the Java SDK's [JavaBatchResponses].
 *
 * The Java SDK uses mutable builder-style `BatchResponse` and `BatchResponses` objects,
 * while the Kotlin API uses immutable data classes. This function bridges between the two.
 */
internal fun List<BatchResponse>.toJavaBatchResponses(): JavaBatchResponses {
    val javaBatchResponses = JavaBatchResponses()
    for (response in this) {
        val javaBatchResponse = JavaBatchResponse(response.id)
        for (msg in response.messages) {
            javaBatchResponse.append(msg.toJavaMessage())
        }
        javaBatchResponses.append(javaBatchResponse)
    }
    return javaBatchResponses
}

/**
 * Convert a Kotlin Message to the Java SDK's [JavaMessage].
 *
 * The Java SDK `Message` constructor expects `(byte[], String[], String[])`,
 * so Kotlin `List<String>` fields are converted via [toTypedArray].
 */
internal fun Message.toJavaMessage(): JavaMessage {
    return JavaMessage(
        this.value,
        this.keys.toTypedArray(),
        this.tags.toTypedArray()
    )
}
