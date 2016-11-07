package com.quest.sparkdemo

/**
  * Created 2016-05-29 9:47 PM by gordon.
  */
class ParsingTests extends UnitSpec {

  behavior of "ParsedLogLine"

  it should "parse an Apache access log line" in {
    val s = "162.17.85.13 - - [31/May/2015:20:46:04 -0400] \"GET /partials/macro.html HTTP/1.0\" 200 394 \"http://ffxiv.lokyst.net/\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.81 Safari/537.36\""
    val r = ParsedLogLine.parse(s).get
    inside(r) {
      case ParsedLogLine(address, identity, user, timestamp, request, status, size, referer, userAgent) =>
        address should be ("162.17.85.13")
        identity should be ("-")
        user should be ("-")
        timestamp should be ("31/May/2015:20:46:04 -0400")
        request should be ("GET /partials/macro.html HTTP/1.0")
        status should be ("200")
        size should be ("394")
        referer should be ("http://ffxiv.lokyst.net/")
        userAgent should be ("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.81 Safari/537.36")
    }
  }

  it should "extract a path from a valid request line" in {
    val s = "162.17.85.13 - - [31/May/2015:20:46:04 -0400] \"GET /partials/macro.html HTTP/1.0\" 200 394 \"http://ffxiv.lokyst.net/\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.81 Safari/537.36\""
    val r = ParsedLogLine.parse(s).get
    r.path should be ("/partials/macro.html")
  }

}
