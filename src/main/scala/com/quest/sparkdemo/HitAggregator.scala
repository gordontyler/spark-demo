package com.quest.sparkdemo

import com.datastax.spark.connector._
import io.undertow.server.{HttpHandler, HttpServerExchange}
import io.undertow.{Handlers, Undertow}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JInt, JObject, JString}
import org.json4s.native.JsonMethods._

/**
  * Created 2016-05-28 10:13 PM by gordon.
  */
object HitAggregator {
  implicit val jsonFormats = DefaultFormats

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("HitAggregator")

    if (conf.get("spark.cassandra.connection.host", null) == null) {
      conf.set("spark.cassandra.connection.host", "localhost")
    }

    val sc = new SparkContext(conf)
    @volatile var server: Undertow = null
    val stopSignal = new Object()

    val routeHandler = Handlers.routing()
      .post("/aggregate", new HttpHandler {
        override def handleRequest(exchange: HttpServerExchange): Unit = {
          try {
            val hits = sc.cassandraTable[Hit]("sparktest", "hits")
            val hitStats = hits.map(h => (h.url, AggregatedHitStats.from(h)))
            val aggregatedHitStats = hitStats.reduceByKey(_ + _).map { case (url, stats) => stats }
            val hitCount = aggregatedHitStats.map(s => s.count + s.error_count).reduce(_ + _)
            val aggCount = aggregatedHitStats.count()
            aggregatedHitStats.saveToCassandra("sparktest", "aggregated_hits")
            exchange.getResponseSender.send(pretty(render(
              JObject(
                "hitCount" -> JInt(hitCount),
                "aggCount" -> JInt(aggCount)
              )
            )))
          }
          catch {
            case e: Exception =>
              exchange.setStatusCode(500)
              exchange.getResponseSender.send(pretty(render(
                JObject(
                  "error" -> JString(e.toString)
                )
              )))
          }
        }
      })
      .post("/shutdown", new HttpHandler {
        override def handleRequest(exchange: HttpServerExchange): Unit = {
          stopSignal.synchronized {
            stopSignal.notifyAll()
          }
        }
      })

    server = Undertow.builder().addHttpListener(8181, "localhost", routeHandler).build()
    server.start()

    stopSignal.synchronized {
      stopSignal.wait()
    }

    server.stop()
    sc.stop()
  }
}
