package com.quest.sparkdemo

/**
 * @author Gordon Tyler
 */
case class AggregatedHitStats(source: String, url: String, count: Int, error_count: Int) {
  def +(o: AggregatedHitStats): AggregatedHitStats =
    copy(count = count + o.count, error_count = error_count + o.error_count)
}

object AggregatedHitStats {
  def from(h: Hit): AggregatedHitStats =
    AggregatedHitStats(h.source, h.url, 1, if (h.status == 200) 0 else 1)
}
