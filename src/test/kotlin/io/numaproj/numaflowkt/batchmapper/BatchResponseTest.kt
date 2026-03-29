package io.numaproj.numaflowkt.batchmapper

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class BatchResponseTest {

    @Test
    fun `data class equality`() {
        val r1 = BatchResponse(
            id = "1",
            messages = listOf(Message(value = "hello".toByteArray()))
        )
        val r2 = BatchResponse(
            id = "1",
            messages = listOf(Message(value = "hello".toByteArray()))
        )
        assertEquals(r1, r2)
    }

    @Test
    fun `default empty messages`() {
        val response = BatchResponse(id = "1")
        assertEquals(emptyList<Message>(), response.messages)
    }
}
