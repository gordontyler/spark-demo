package com.quest.sparkdemo

/**
  * Created 2016-11-07 10:25 AM by gordon.
  */
case class ParsedLogLine(
  address: String,
  identity: String,
  user: String,
  timestamp: String,
  request: String,
  status: String,
  size: String,
  referer: String,
  userAgent: String
) {
  def path: String = {
    import ParsedLogLine.RequestPattern
    request match {
      case RequestPattern(_, path, _) => path
      case _ => ""
    }
  }

}

object ParsedLogLine {
  val RequestPattern = "(HEAD|GET|POST|PUT|DELETE) (.+) HTTP/(1.0|1.1)".r
  val LogLine = "^(.+?) (.+?) (.+?) \\[(.+?)\\] \"(.+?)\" (.+?) (.+?)(?: \"(.+?)\")?(?: \"(.+?)\")?$".r

  def parse(s: String): Option[ParsedLogLine] = {
    s match {
      case LogLine(address, identity, user, timestamp, request, status, size, referer, userAgent) =>
        Some(ParsedLogLine(address, identity, user, timestamp, request, status, size, referer, userAgent))
      case _ => None
    }
  }
}
