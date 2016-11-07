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

    if (conf.get("spark.cassandra.connection.host", null) == null) {
      conf.set("spark.cassandra.connection.host", "localhost")
    }

    val sc = new SparkContext(conf)

    val hits = sc.cassandraTable[Hit]("sparktest", "hits")
    val hitStats = hits.map(h => (h.url, AggregatedHitStats.from(h)))
    val aggregatedHitStats = hitStats.reduceByKey(_ + _).map{ case (url, stats) => stats}

    aggregatedHitStats.saveToCassandra("sparktest", "aggregated_hits")
  }
}
