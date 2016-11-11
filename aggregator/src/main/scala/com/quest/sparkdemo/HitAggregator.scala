package com.quest.sparkdemo

import com.datastax.spark.connector._
import io.undertow.server.{HttpHandler, HttpServerExchange}
import io.undertow.{Handlers, Undertow}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.streaming.Seconds
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
    val session = SparkSession.builder().config(conf).getOrCreate()
    val stopSignal = new Object()

    val routeHandler = Handlers.routing()
      .post("/aggregate", new HttpHandler {
        override def handleRequest(exchange: HttpServerExchange): Unit = {
          try {
            val sql = exchange.getQueryParameters.containsKey("sql")
            val result = if (sql) aggregateSQL(session) else aggregate(sc)

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
    session.stop()
    sc.stop()
  }

  def aggregate(sc: SparkContext): AggregateResult = {
    val hitsTable = sc
      .cassandraTable("sparkdemo", "hits")
      .select("url", "status")
      .as(HitSubset)

    val hitStatsByURL = hitsTable.map(h => (h.url, HitStats(h.url, 1, if (h.status == 200) 0 else 1)))
    val aggregatedHitStatsByURL = hitStatsByURL.reduceByKey((hs1, hs2) => hs1 + hs2)
    val aggregatedHitStats = aggregatedHitStatsByURL.map { case (url, hs) => hs }
    val hitCount = aggregatedHitStats.aggregate(0L)(
      (c, hs) => c + hs.count,
      (c1, c2) => c1 + c2
    )
    val aggCount = aggregatedHitStats.count()
    aggregatedHitStats.saveToCassandra("sparkdemo", "aggregated_hits")

    AggregateResult(hitCount, aggCount)
  }

  def aggregateSQL(session: SparkSession): AggregateResult = {
    import org.apache.spark.sql.expressions.scalalang.typed._
    import session.implicits._

    val hitsTable = session.sparkContext
      .cassandraTable("sparkdemo", "hits")
      .select("url", "status")
      .as(HitSubset)

    val hitsDS = session.createDataset(hitsTable)

    val aggHits = hitsDS
      .map(h => HitStats(h.url, 1, if (h.status == 200) 0 else 1))
      .groupByKey(_.url)
      .agg(sumLong[HitStats](_.count).name("count"),
           sumLong[HitStats](_.error_count).name("error_count"))
      .withColumnRenamed("value", "url")
      .as[HitStats]

    val hitCount = hitsDS.count()
    val aggCount = aggHits.count()

    aggHits.rdd.saveToCassandra("sparkdemo", "aggregated_hits")

    AggregateResult(hitCount, aggCount)
  }

  case class AggregateResult(hitCount: Long, aggCount: Long)
  case class HitSubset(url: String, status: Int)
}
