package org.example

import com.hazelcast.core.HazelcastInstance
import java.nio.file.Files
import java.nio.file.Paths
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

class ClientRequestMonitor(val instance: HazelcastInstance, val secondsToRun: Int, val threads: Int) : Runnable {
    class CsvEntry(val timestamp: Long, val opsThisSecond: Long, val slowThisSecond: Long)

    override fun run() {
        val opsCounter = AtomicLong()
        val slowOpsCounter = AtomicLong()

        var localOpsCounterView = opsCounter.get()
        var localSlowOpsCounterView = slowOpsCounter.get()

        val keepRunning = AtomicBoolean(true)
        val threadHandles = ArrayList<Thread>()
        val csvRows = ArrayList<CsvEntry>()
        for (i in 0 until threads) {
            val t = Thread(MapQueryRunner(instance, keepRunning, opsCounter, slowOpsCounter))
            t.isDaemon = true
            t.start()
            threadHandles.add(t)
        }

        for (i in 0 until secondsToRun) {
            try {
                TimeUnit.SECONDS.sleep(1)
            } catch (e: InterruptedException) {
                e.printStackTrace()
            }

            // approximations...
            val opsCounterObserved = opsCounter.get()
            val opsPerSecond = opsCounterObserved - localOpsCounterView

            val slowOpsCounterObserved = slowOpsCounter.get()
            val slowOpsPerSecond = slowOpsCounterObserved - localSlowOpsCounterView
            csvRows.add(CsvEntry(System.currentTimeMillis(), opsPerSecond, slowOpsPerSecond))
            localOpsCounterView = opsCounterObserved
            localSlowOpsCounterView = slowOpsCounterObserved
        }

        println("Shutting down worker threads")
        keepRunning.set(false)
        threadHandles.forEach { it.join() }

        writeCsv(csvRows)
    }

    private fun writeCsv(csvRows: List<CsvEntry>) {
        val sb = StringBuilder()
        sb.append("timestamp,ops/s,slow ops/s\n")
        csvRows.forEach {
            sb.append("${it.timestamp},${it.opsThisSecond},${it.slowThisSecond}\n")
        }
        val file = "${UUID.randomUUID()}.csv"
        Files.writeString(Paths.get(file), sb)
        println("CSV: $file")
    }
}