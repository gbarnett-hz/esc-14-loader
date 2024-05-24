package org.example

import com.hazelcast.client.HazelcastClient
import com.hazelcast.client.config.ClientConfig
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

    val src = "map0"
    when (cmd) {
        "seed" -> {
            val map = client.getMap<String, String>(src)
            val items = HashMap<String, String>()
            val value =
                "dfgshjfdfgshjfhjdfsgjkldfgkldfsjgdflkgjdfklgjdflkgjdflskgjdfsklgdjslgkdsfjgkdfglsgjghjghjghfjgjgjghjhjdfsgjkldfgkldfsjgdflkgjdfklgjdflkgjdflskgjdfsklgdjslgkdsfjgkdfglsgjghjghjghfjgjgjghj"
            for (i in 0 until 100_000) {
                items["k$i"] = value
            }
            map.setAll(items)
        }
        "copy" -> {
            val counter = AtomicInteger()
            val fs = ArrayList<Future<*>>()
            val es = Executors.newFixedThreadPool(32)
            val copies = 150
            for (i in 1 .. copies) {
                fs.add(es.submit(Replicator(client, counter, src, "map$i")))
            }

            while (!fs.all { it.isDone }) {
                println("$counter / $copies")
                TimeUnit.SECONDS.sleep(5)
            }

            println("Complete: $counter / $copies")
        }
        else -> {
            println("Unknown command: $cmd")
        }
    }
    client.shutdown()
}