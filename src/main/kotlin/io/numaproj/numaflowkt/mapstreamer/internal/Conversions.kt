package io.numaproj.numaflowkt.mapstreamer.internal

import io.numaproj.numaflowkt.mapstreamer.Datum
import io.numaproj.numaflowkt.mapstreamer.Message
import io.numaproj.numaflow.mapstreamer.Datum as JavaDatum
import io.numaproj.numaflow.mapstreamer.Message as JavaMessage

/**
 * Convert Java SDK Datum to Kotlin Datum.
 *
 * Null-coalesces defensively: the Java SDK does not annotate nullability,
 * so `value` and `headers` may arrive as `null` despite being logically non-null.
 */
internal fun JavaDatum.toKotlinDatum(): Datum = Datum(
    value = this.value ?: byteArrayOf(),
    eventTime = this.eventTime,
    watermark = this.watermark,
    headers = this.headers ?: emptyMap()
)

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
