package com.quest.sparkdemo

import java.io.File

import com.datastax.driver.core.utils.UUIDs
import com.datastax.spark.connector.streaming._
import io.undertow.server.{HttpHandler, HttpServerExchange}
import io.undertow.{Handlers, Undertow}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * Created 2016-05-29 7:09 PM by gordon.
  */
object HitLoader {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("HitLoader")
      .set("spark.cassandra.connection.keep_alive_ms", "20000")

    if (conf.get("spark.cassandra.connection.host", null) == null) {
      conf.set("spark.cassandra.connection.host", "localhost")
    }

    val ssc = new StreamingContext(conf, Duration(10000))

    val inputsDir: File = new File(System.getenv("HOME"), "dev/spark-test/inputs")
    val input = ssc.textFileStream(inputsDir.toString)
    input.print()

    val lines = input.flatMap(ParsedLogLine.parse)
    lines.print()

    val hits = lines.map(l => Hit(UUIDs.timeBased(), l.address, l.path, l.status.toInt))
    hits.print()

    hits.saveToCassandra("sparktest", "hits")

    @volatile var server: Undertow = null
    val stopSignal = new Object()

    val routeHandler = Handlers.routing()
      .post("/shutdown", new HttpHandler {
        override def handleRequest(exchange: HttpServerExchange): Unit = {
          stopSignal.synchronized {
            stopSignal.notifyAll()
          }
        }
      })

    server = Undertow.builder().addHttpListener(8181, "localhost", routeHandler).build()
    server.start()

    ssc.start()

    stopSignal.synchronized {
      stopSignal.wait()
    }

    server.stop()
    ssc.stop()
  }

}
