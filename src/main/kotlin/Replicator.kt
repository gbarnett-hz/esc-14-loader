package org.example

import com.hazelcast.core.HazelcastInstance
import java.util.concurrent.atomic.AtomicInteger

class Replicator(val client: HazelcastInstance, val counter: AtomicInteger, val src: String, val dest: String) : Runnable {
    override fun run() {
        val src = client.getMap<String, String>(src)
        val map = client.getMap<String, String>(dest)
        map.setAll(src)
        counter.incrementAndGet()
    }
}