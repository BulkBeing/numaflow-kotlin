package io.numaproj.numaflowkt.sinker.internal

import io.numaproj.numaflow.sinker.DatumIterator
import io.numaproj.numaflow.sinker.ResponseList
import io.numaproj.numaflowkt.sinker.Datum
import io.numaproj.numaflowkt.sinker.Message
import io.numaproj.numaflowkt.sinker.Response
import io.numaproj.numaflowkt.sinker.UserMetadata
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import io.numaproj.numaflow.sinker.Datum as JavaDatum
import io.numaproj.numaflow.sinker.Response as JavaResponse
import io.numaproj.numaflow.sinker.Message as JavaMessage

// ---------------------------------------------------------------------------
// DatumIterator -> Flow<Datum>
// ---------------------------------------------------------------------------

/**
 * Converts a blocking Java [DatumIterator] into a Kotlin [Flow] of [Datum].
 *
 * Each Java Datum is eagerly converted to a Kotlin [Datum] data class
 * as it arrives. The blocking [DatumIterator.next] call is acceptable here
 * because the Java SDK already calls [processMessages] on its own thread pool,
 * so this flow runs on a pool thread (not the main dispatcher).
 *
 * The flow completes when [DatumIterator.next] returns `null`, which signals
 * end-of-batch (the Java SDK's EOF_DATUM sentinel was received).
 */
internal fun DatumIterator.asFlow(): Flow<Datum> = flow {
    while (true) {
        val javaDatum = next() ?: break
        emit(javaDatum.toKotlinDatum())
    }
}

// ---------------------------------------------------------------------------
// Java Datum -> Kotlin Datum
// ---------------------------------------------------------------------------

/**
 * Eagerly converts a Java [Datum][JavaDatum] to a Kotlin [Datum] data class.
 *
 * The Java SDK's Datum interface exposes `String[]` for keys, which we convert
 * to `List<String>`. `null` arrays/maps from the Java side are normalized to
 * empty collections in the Kotlin data class.
 *
 * Note: Java SDK v0.11.0 Datum does not expose userMetadata or systemMetadata.
 */
internal fun JavaDatum.toKotlinDatum(): Datum = Datum(
    id = getId(),
    value = getValue(),
    keys = getKeys()?.toList() ?: emptyList(),
    eventTime = getEventTime(),
    watermark = getWatermark(),
    headers = getHeaders() ?: emptyMap()
)

// ---------------------------------------------------------------------------
// Kotlin Response -> Java Response
// ---------------------------------------------------------------------------

/**
 * Converts a list of Kotlin [Response]s to a Java [ResponseList].
 */
internal fun List<Response>.toJavaResponseList(): ResponseList {
    val builder = ResponseList.newBuilder()
    for (response in this) {
        builder.addResponse(response.toJavaResponse())
    }
    return builder.build()
}

/**
 * Converts a single Kotlin [Response] to a Java [Response][JavaResponse].
 *
 * Maps each sealed subtype to the corresponding Java factory method:
 * - [Response.Ok] -> `Response.responseOK()`
 * - [Response.Failure] -> `Response.responseFailure()`
 * - [Response.Fallback] -> `Response.responseFallback()`
 * - [Response.Serve] -> `Response.responseServe()`
 * - [Response.OnSuccess] -> `Response.responseOnSuccess()`
 */
internal fun Response.toJavaResponse(): JavaResponse = when (this) {
    is Response.Ok -> JavaResponse.responseOK(id)
    is Response.Failure -> JavaResponse.responseFailure(id, error)
    is Response.Fallback -> JavaResponse.responseFallback(id)
    is Response.Serve -> JavaResponse.responseServe(id, payload)
    is Response.OnSuccess -> JavaResponse.responseOnSuccess(id, message.toJavaMessage())
}

// ---------------------------------------------------------------------------
// Kotlin Message -> Java Message
// ---------------------------------------------------------------------------

/**
 * Converts a Kotlin [Message] to a Java [Message][JavaMessage].
 *
 * Uses the Java SDK's Lombok `@Builder` pattern to construct the Java Message
 * with value, key (singular), and optional userMetadata.
 */
internal fun Message.toJavaMessage(): JavaMessage = JavaMessage.builder()
    .value(value)
    .key(key)
    .userMetadata(userMetadata?.toJavaUserMetadata())
    .build()

// ---------------------------------------------------------------------------
// Kotlin UserMetadata -> Java HashMap<String, KeyValueGroup>
// ---------------------------------------------------------------------------

/**
 * Converts a Kotlin [UserMetadata] to the Java SDK's metadata structure.
 *
 * The Java SDK's [Message] stores user metadata as `HashMap<String, KeyValueGroup>`
 * where `KeyValueGroup` wraps `HashMap<String, byte[]>`.
 */
internal fun UserMetadata.toJavaUserMetadata(): HashMap<String, io.numaproj.numaflow.sinker.KeyValueGroup> {
    val result = HashMap<String, io.numaproj.numaflow.sinker.KeyValueGroup>()
    for (group in groups()) {
        val kvMap = HashMap<String, ByteArray>()
        for (key in keys(group)) {
            val value = get(group, key) ?: continue
            kvMap[key] = value
        }
        result[group] = io.numaproj.numaflow.sinker.KeyValueGroup.builder().keyValue(kvMap).build()
    }
    return result
}
