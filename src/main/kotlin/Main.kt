package org.example

import com.hazelcast.client.HazelcastClient
import com.hazelcast.client.config.ClientConfig
import java.util.Random
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

fun main(args: Array<String>) {
    val cmd = args[1]
    val ips = args[0].split(",")
    val cfg = ClientConfig()
    cfg.networkConfig.setAddresses(ips)
    val client = HazelcastClient.newHazelcastClient(cfg)

    val src = "map"
    when (cmd) {
        "seed" -> {
            val valueSizeBytes = Integer.parseInt(args[2])
            val keys = Integer.parseInt(args[3])
            val v = ByteArray(valueSizeBytes)
            Random().nextBytes(v)
            val map = client.getMap<String, ByteArray>(src)
            val items = HashMap<String, ByteArray>()
            for (i in 0 until keys) {
                items["k$i"] = v
            }
            map.setAll(items)
        }
        "copy" -> {
            val copies = Integer.parseInt(args[2])
            val counter = AtomicInteger()
            val fs = ArrayList<Future<*>>()
            val es = Executors.newFixedThreadPool(32)
            for (i in 1 .. copies) {
                fs.add(es.submit(Replicator(client, counter, src, "${UUID.randomUUID()}")))
            }

            while (!fs.all { it.isDone }) {
                println("$counter / $copies")
                TimeUnit.SECONDS.sleep(5)
            }

            println("Complete: $counter / $copies")
            es.shutdown()
        }
        "client" -> {
            val secondsToRun = Integer.parseInt(args[2])
            val threads = Integer.parseInt(args[3])
            val crm = ClientRequestMonitor(client, secondsToRun, threads)
            crm.run()
        }
        "map" -> {
            val m = client.getMap<String, String>("map-native")
            println(m.size)
        }
        else -> {
            println("Unknown command: $cmd")
        }
    }
    client.shutdown()
}