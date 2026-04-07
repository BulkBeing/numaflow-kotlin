package io.numaproj.numaflowkt.sourcer

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class OffsetTest {

    @Test
    fun `offset equality compares value by content`() {
        val o1 = Offset(value = "abc".toByteArray(), partitionId = 1)
        val o2 = Offset(value = "abc".toByteArray(), partitionId = 1)
        assertEquals(o1, o2)
        assertEquals(o1.hashCode(), o2.hashCode())
    }

    @Test
    fun `offsets with different content are not equal`() {
        val o1 = Offset(value = "abc".toByteArray(), partitionId = 1)
        val o2 = Offset(value = "xyz".toByteArray(), partitionId = 1)
        assertNotEquals(o1, o2)
    }

    @Test
    fun `offsets with different partitionIds are not equal`() {
        val o1 = Offset(value = "abc".toByteArray(), partitionId = 1)
        val o2 = Offset(value = "abc".toByteArray(), partitionId = 2)
        assertNotEquals(o1, o2)
    }

    @Test
    fun `default partitionId uses cached defaultPartitions`() {
        val o = Offset(value = "test".toByteArray())
        assertEquals(Sourcer.defaultPartitions().first(), o.partitionId)
    }
}
