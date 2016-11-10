package com.quest.sparkdemo

import org.scalatest._

/**
  * Created 2016-05-29 9:51 PM by gordon.
  */
abstract class UnitSpec
  extends FlatSpec
    with Matchers
    with OptionValues
    with Inside
    with Inspectors

