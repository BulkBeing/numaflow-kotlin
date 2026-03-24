package io.numaproj.numaflowkt.sinker

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class ResponseTest {

    @Test
    fun `Ok equality`() {
        assertEquals(Response.Ok("1"), Response.Ok("1"))
        assertNotEquals(Response.Ok("1"), Response.Ok("2"))
    }

    @Test
    fun `Failure equality`() {
        assertEquals(
            Response.Failure("1", "err"),
            Response.Failure("1", "err")
        )
        assertNotEquals(
            Response.Failure("1", "err1"),
            Response.Failure("1", "err2")
        )
    }

    @Test
    fun `Serve compares payload by content`() {
        val r1 = Response.Serve("1", "data".toByteArray())
        val r2 = Response.Serve("1", "data".toByteArray())
        assertEquals(r1, r2)
        assertEquals(r1.hashCode(), r2.hashCode())
    }

    @Test
    fun `Serve with different payloads are not equal`() {
        val r1 = Response.Serve("1", "abc".toByteArray())
        val r2 = Response.Serve("1", "xyz".toByteArray())
        assertNotEquals(r1, r2)
    }

    @Test
    fun `OnSuccess with message`() {
        val msg = Message(value = "forwarded".toByteArray())
        val r = Response.OnSuccess("1", msg)
        assertEquals(msg, r.message)
    }

    @Test
    fun `OnSuccess equality`() {
        val msg = Message(value = "data".toByteArray())
        assertEquals(Response.OnSuccess("1", msg), Response.OnSuccess("1", msg))
    }

    @Test
    fun `Fallback equality`() {
        assertEquals(Response.Fallback("1"), Response.Fallback("1"))
        assertNotEquals(Response.Fallback("1"), Response.Fallback("2"))
    }

    @Test
    fun `different response types are not equal`() {
        assertNotEquals(Response.Ok("1") as Response, Response.Failure("1", "") as Response)
        assertNotEquals(Response.Ok("1") as Response, Response.Fallback("1") as Response)
    }

    @Test
    fun `exhaustive when matching compiles`() {
        val response: Response = Response.Ok("1")
        val label = when (response) {
            is Response.Ok -> "ok"
            is Response.Failure -> "failure"
            is Response.Fallback -> "fallback"
            is Response.Serve -> "serve"
            is Response.OnSuccess -> "onSuccess"
        }
        assertEquals("ok", label)
    }
}
