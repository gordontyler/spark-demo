package com.quest.sparkdemo

/**
 * @author Gordon Tyler
 */
case class HitStats(source: String, url: String, count: Int, error_count: Int) {
  def +(o: HitStats): HitStats =
    copy(count = count + o.count, error_count = error_count + o.error_count)
}

object HitStats {
  def from(h: Hit): HitStats =
    HitStats(h.source, h.url, 1, if (h.status == 200) 0 else 1)
}
