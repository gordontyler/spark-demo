package com.quest.sparkdemo

import com.datastax.spark.connector._
import io.undertow.server.{HttpHandler, HttpServerExchange}
import io.undertow.{Handlers, Undertow}
import org.apache.spark.streaming.Seconds
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
      .set("spark.cassandra.connection.keep_alive_ms", Seconds(30).milliseconds.toString)

    if (conf.get("spark.cassandra.connection.host", null) == null) {
      conf.set("spark.cassandra.connection.host", "localhost")
    }

    val sc = new SparkContext(conf)
    val stopSignal = new Object()

    val routeHandler = Handlers.routing()
      .post("/aggregate", new HttpHandler {
        override def handleRequest(exchange: HttpServerExchange): Unit = {
          try {
            val result = aggregate(sc)

            exchange.getResponseSender.send(pretty(render(
              JObject(
                "hitCount" -> JInt(result.hitCount),
                "aggCount" -> JInt(result.aggCount)
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

    val server = Undertow.builder().addHttpListener(8282, "localhost", routeHandler).build()
    server.start()

    stopSignal.synchronized {
      stopSignal.wait()
    }

    Thread.sleep(1000)

    server.stop()
    sc.stop()
  }

  def aggregate(sc: SparkContext): AggregateResult = {
    val hits = sc.cassandraTable[Hit]("sparkdemo", "hits")

    val hitStatsByURL = hits.map(hit => (hit.url, HitStats.from(hit)))
    val aggregatedHitStatsByURL = hitStatsByURL.reduceByKey((hs1, hs2) => hs1 + hs2)
    val aggregatedHitStats = aggregatedHitStatsByURL.map { case (url, hs) => hs }
    val hitCount = aggregatedHitStats.aggregate(0)(
      (c, hs) => c + hs.count + hs.error_count,
      (c1, c2) => c1 + c2
    )
    val aggCount = aggregatedHitStats.count()
    aggregatedHitStats.saveToCassandra("sparkdemo", "aggregated_hits")

    AggregateResult(hitCount, aggCount)
  }

  case class AggregateResult(hitCount: Int, aggCount: Long)
}
