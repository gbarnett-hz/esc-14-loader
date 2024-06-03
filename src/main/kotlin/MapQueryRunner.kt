package org.example

import com.hazelcast.core.HazelcastInstance
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

class MapQueryRunner(
    private val instance: HazelcastInstance, private val keepRunning: AtomicBoolean, private val counter: AtomicLong,
    private val slowOps: AtomicLong) : Runnable {
    override fun run() {
        while (keepRunning.get()) {
            for (mapSuffix in 1 .. 150) {
                val map = instance.getMap<String,Int>("map$mapSuffix")
                for (i in 0 until 100) {
                    val start = Instant.now()
                    try {
                        map.set("k$i", i);
                        counter.incrementAndGet()
                    } catch (e: Exception) {
                        println("Error: ${e.message}")
                    }
                    val stop = Instant.now()
                    val duration = Duration.between(start, stop).toMillis()
                    if (duration > 500) {
                        slowOps.incrementAndGet()
                    }
                }
            }
        }
    }
}