package io.numaproj.numaflowkt.mapper.internal

import io.numaproj.numaflowkt.mapper.Datum
import io.numaproj.numaflowkt.mapper.Message
import io.numaproj.numaflow.mapper.Datum as JavaDatum
import io.numaproj.numaflow.mapper.Message as JavaMessage
import io.numaproj.numaflow.mapper.MessageList as JavaMessageList

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
 * Convert a list of Kotlin Messages to the Java SDK's [JavaMessageList].
 *
 * The Java SDK uses a builder-based `MessageList` rather than a plain `List`,
 * so we iterate and append each converted message.
 */
internal fun List<Message>.toJavaMessageList(): JavaMessageList {
    val builder = JavaMessageList.newBuilder()
    for (msg in this) {
        builder.addMessage(msg.toJavaMessage())
    }
    return builder.build()
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
