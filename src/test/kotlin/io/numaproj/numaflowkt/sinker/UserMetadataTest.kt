package io.numaproj.numaflowkt.sinker

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class UserMetadataTest {

    @Test
    fun `put and get round-trip`() {
        val meta = UserMetadata()
        meta.put("group1", "key1", "value1".toByteArray())

        assertArrayEquals("value1".toByteArray(), meta.get("group1", "key1"))
    }

    @Test
    fun `get returns null for missing group`() {
        val meta = UserMetadata()
        assertNull(meta.get("missing", "key"))
    }

    @Test
    fun `get returns null for missing key`() {
        val meta = UserMetadata()
        meta.put("group1", "key1", "value1".toByteArray())
        assertNull(meta.get("group1", "missing"))
    }

    @Test
    fun `remove key`() {
        val meta = UserMetadata()
        meta.put("g", "k1", "v1".toByteArray())
        meta.put("g", "k2", "v2".toByteArray())
        meta.remove("g", "k1")

        assertNull(meta.get("g", "k1"))
        assertNotNull(meta.get("g", "k2"))
    }

    @Test
    fun `removeGroup removes entire group`() {
        val meta = UserMetadata()
        meta.put("g", "k1", "v1".toByteArray())
        meta.put("g", "k2", "v2".toByteArray())
        meta.removeGroup("g")

        assertNull(meta.get("g", "k1"))
        assertNull(meta.get("g", "k2"))
        assertTrue(meta.groups().isEmpty())
    }

    @Test
    fun `groups and keys enumeration`() {
        val meta = UserMetadata()
        meta.put("g1", "a", "1".toByteArray())
        meta.put("g1", "b", "2".toByteArray())
        meta.put("g2", "c", "3".toByteArray())

        assertEquals(setOf("g1", "g2"), meta.groups())
        assertEquals(setOf("a", "b"), meta.keys("g1"))
        assertEquals(setOf("c"), meta.keys("g2"))
        assertEquals(emptySet<String>(), meta.keys("missing"))
    }

    @Test
    fun `clear removes all`() {
        val meta = UserMetadata()
        meta.put("g", "k", "v".toByteArray())
        meta.clear()

        assertTrue(meta.groups().isEmpty())
    }

    @Test
    fun `equality compares byte array content`() {
        val m1 = UserMetadata()
        m1.put("g", "k", "value".toByteArray())
        val m2 = UserMetadata()
        m2.put("g", "k", "value".toByteArray())
        assertEquals(m1, m2)
    }
}
