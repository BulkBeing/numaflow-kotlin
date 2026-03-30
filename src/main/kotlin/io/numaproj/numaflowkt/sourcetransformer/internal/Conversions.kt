package io.numaproj.numaflowkt.sourcetransformer.internal

import io.numaproj.numaflowkt.sourcetransformer.Datum
import io.numaproj.numaflowkt.sourcetransformer.Message
import io.numaproj.numaflow.sourcetransformer.Datum as JavaDatum
import io.numaproj.numaflow.sourcetransformer.Message as JavaMessage
import io.numaproj.numaflow.sourcetransformer.MessageList as JavaMessageList

/**
 * Convert Java SDK Datum to Kotlin Datum.
 *
 * The Java SDK's [JavaDatum] interface exposes `getValue()`, `getEventTime()`,
 * `getWatermark()`, and `getHeaders()`. Event time and watermark are guaranteed
 * non-null by the protobuf contract (proto3 message fields always have a default).
 * We null-coalesce `value` and `headers` defensively since the Java SDK does not
 * annotate nullability on those.
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
 * The Java SDK's source transformer Message constructor is:
 *   `Message(byte[] value, Instant eventTime, String[] keys, String[] tags)`
 *
 * Note: unlike the mapper's Message, this includes eventTime as the second parameter.
 * This is the critical difference — source transformer messages carry event times
 * for watermark progression.
 */
internal fun Message.toJavaMessage(): JavaMessage {
    return JavaMessage(
        this.value,
        this.eventTime,
        this.keys.toTypedArray(),
        this.tags.toTypedArray()
    )
}
