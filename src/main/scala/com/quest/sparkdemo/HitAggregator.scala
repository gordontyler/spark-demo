package com.quest.sparkdemo

import com.datastax.spark.connector._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created 2016-05-28 10:13 PM by gordon.
  */
object HitAggregator {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("HitAggregator")
      .set("spark.cassandra.connection.host", "localhost")
    val sc = new SparkContext(conf)

    val hits = sc.cassandraTable[Hit]("sparktest", "hits")
    val hitStats = hits.map(h => (h.url, AggregatedHitStats.from(h)))
    val aggregatedHitStats = hitStats.reduceByKey(_ + _).map{ case (url, stats) => stats}

    aggregatedHitStats.saveToCassandra("sparktest", "aggregated_hits")
  }
}

case class AggregatedHitStats(source: String, url: String, count: Int, error_count: Int) {
  def +(o: AggregatedHitStats): AggregatedHitStats =
    copy(count = count + o.count, error_count = error_count + o.error_count)
}

object AggregatedHitStats {
  def from(h: Hit): AggregatedHitStats =
    AggregatedHitStats(h.source, h.url, 1, if (h.status == 200) 0 else 1)
}