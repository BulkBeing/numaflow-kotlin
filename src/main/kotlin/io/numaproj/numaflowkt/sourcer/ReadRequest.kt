package io.numaproj.numaflowkt.sourcer

import kotlin.time.Duration

/**
 * Parameters for a [Sourcer.read] call.
 *
 * @property count   Maximum number of messages to read in this batch.
 * @property timeout Maximum time to wait for messages before returning.
 *                   Uses [kotlin.time.Duration] for Kotlin idiom.
 */
data class ReadRequest(
    val count: Long,
    val timeout: Duration
)
