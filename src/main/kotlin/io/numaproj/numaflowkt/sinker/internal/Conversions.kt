package io.numaproj.numaflowkt.sinker.internal

import io.numaproj.numaflow.sinker.DatumIterator
import io.numaproj.numaflow.sinker.ResponseList
import io.numaproj.numaflowkt.sinker.Datum
import io.numaproj.numaflowkt.sinker.Message
import io.numaproj.numaflowkt.sinker.Response
import io.numaproj.numaflowkt.sinker.SystemMetadata
import io.numaproj.numaflowkt.sinker.UserMetadata
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import io.numaproj.numaflow.sinker.Datum as JavaDatum
import io.numaproj.numaflow.sinker.Response as JavaResponse
import io.numaproj.numaflow.sinker.Message as JavaMessage

// ---------------------------------------------------------------------------
// DatumIterator → Flow<Datum>
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
// Java Datum → Kotlin Datum
// ---------------------------------------------------------------------------

/**
 * Eagerly converts a Java [Datum][JavaDatum] to a Kotlin [Datum] data class.
 *
 * The Java SDK's Datum interface exposes `String[]` for keys, which we convert
 * to `List<String>`. Metadata objects are converted to their Kotlin counterparts.
 * `null` fields from the Java side are preserved as `null` in the Kotlin data class.
 */
internal fun JavaDatum.toKotlinDatum(): Datum {
    val javaUserMeta: io.numaproj.numaflow.shared.UserMetadata? = getUserMetadata()
    val javaSysMeta: io.numaproj.numaflow.shared.SystemMetadata? = getSystemMetadata()
    return Datum(
        id = getId(),
        value = getValue(),
        keys = getKeys()?.toList(),
        eventTime = getEventTime(),
        watermark = getWatermark(),
        headers = getHeaders(),
        userMetadata = javaUserMeta?.toKotlinUserMetadata(),
        systemMetadata = javaSysMeta?.toKotlinSystemMetadata()
    )
}

// ---------------------------------------------------------------------------
// Metadata conversions
// ---------------------------------------------------------------------------

/**
 * Converts a Java [UserMetadata][io.numaproj.numaflow.shared.UserMetadata] to a Kotlin [UserMetadata].
 *
 * Iterates over all groups and keys in the Java metadata, reading values
 * via `getValue()` (which returns cloned byte arrays).
 */
internal fun io.numaproj.numaflow.shared.UserMetadata.toKotlinUserMetadata(): UserMetadata {
    val meta = UserMetadata()
    for (group in this.getGroups()) {
        for (key in this.getKeys(group)) {
            val value = this.getValue(group, key) ?: continue
            meta.put(group, key, value)
        }
    }
    return meta
}

/**
 * Converts a Java [SystemMetadata][io.numaproj.numaflow.shared.SystemMetadata] to a Kotlin [SystemMetadata].
 *
 * Iterates over all groups and keys in the Java metadata, building an
 * immutable map structure for the read-only Kotlin counterpart.
 */
internal fun io.numaproj.numaflow.shared.SystemMetadata.toKotlinSystemMetadata(): SystemMetadata {
    val groups = mutableMapOf<String, Map<String, ByteArray>>()
    for (group in this.getGroups()) {
        val keys = mutableMapOf<String, ByteArray>()
        for (key in this.getKeys(group)) {
            val value = this.getValue(group, key) ?: continue
            keys[key] = value
        }
        groups[group] = keys
    }
    return SystemMetadata(groups)
}

/**
 * Converts a Kotlin [UserMetadata] to a Java [UserMetadata][io.numaproj.numaflow.shared.UserMetadata].
 *
 * Used when converting [Message] for [Response.OnSuccess] back to the Java SDK.
 */
internal fun UserMetadata.toJavaUserMetadata(): io.numaproj.numaflow.shared.UserMetadata {
    val javaMeta = io.numaproj.numaflow.shared.UserMetadata()
    for (group in groups()) {
        for (key in keys(group)) {
            val value = get(group, key) ?: continue
            javaMeta.addKV(group, key, value)
        }
    }
    return javaMeta
}

// ---------------------------------------------------------------------------
// Kotlin Response → Java Response
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
 * - [Response.Ok] → `Response.responseOK()`
 * - [Response.Failure] → `Response.responseFailure()`
 * - [Response.Fallback] → `Response.responseFallback()`
 * - [Response.Serve] → `Response.responseServe()`
 * - [Response.OnSuccess] → `Response.responseOnSuccess()`
 */
internal fun Response.toJavaResponse(): JavaResponse = when (this) {
    is Response.Ok -> JavaResponse.responseOK(id)
    is Response.Failure -> JavaResponse.responseFailure(id, error)
    is Response.Fallback -> JavaResponse.responseFallback(id)
    is Response.Serve -> JavaResponse.responseServe(id, payload)
    is Response.OnSuccess -> JavaResponse.responseOnSuccess(id, message?.toJavaMessage())
}

// ---------------------------------------------------------------------------
// Kotlin Message → Java Message
// ---------------------------------------------------------------------------

/**
 * Converts a Kotlin [Message] to a Java [Message][JavaMessage].
 *
 * Selects the appropriate Java constructor based on which optional fields are present.
 * The Java [Message][JavaMessage] has three constructors:
 * - `Message(byte[])` — value only
 * - `Message(byte[], String[])` — value + keys
 * - `Message(byte[], String[], UserMetadata)` — value + keys + metadata
 */
internal fun Message.toJavaMessage(): JavaMessage {
    val javaKeys: Array<String>? = keys?.toTypedArray()
    val javaMeta: io.numaproj.numaflow.shared.UserMetadata? = userMetadata?.toJavaUserMetadata()
    return when {
        javaKeys != null && javaMeta != null -> JavaMessage(value, javaKeys, javaMeta)
        javaKeys != null -> JavaMessage(value, javaKeys)
        else -> JavaMessage(value)
    }
}
