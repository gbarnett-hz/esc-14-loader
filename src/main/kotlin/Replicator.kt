package org.example

import com.hazelcast.core.HazelcastInstance
import java.util.concurrent.atomic.AtomicInteger

class Replicator(private val client: HazelcastInstance,
                 private val counter: AtomicInteger,
                 private val src: String,
                 private val dest: String) : Runnable {
    override fun run() {
        val srcMap = client.getMap<String, ByteArray>(src)
        val dstMap = client.getMap<String, ByteArray>(dest)
        dstMap.setAll(srcMap)
        counter.incrementAndGet()
    }
}