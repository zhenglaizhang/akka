/**
 * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.util

import org.scalatest.FlatSpec
import org.scalatest.Matchers

class PrettyDurationSpec extends FlatSpec with Matchers {

  behavior of "PrettyDuration"

  import akka.util.PrettyDuration._

  import scala.concurrent.duration._

  val cases: Seq[(Duration, String)] =
    9.nanos -> "9.000 ns" ::
      95.nanos -> "95.00 ns" ::
      999.nanos -> "999.0 ns" ::
      1000.nanos -> "1.000 μs" ::
      9500.nanos -> "9.500 μs" ::
      9500.micros -> "9.500 ms" ::
      9500.millis -> "9.500 s" ::
      95.seconds -> "1.583 min" ::
      95.minutes -> "1.583 h" ::
      95.hours -> "3.958 d" ::
      Nil

  cases foreach {
    case (d, expectedValue) ⇒
      it should s"print $d nanos as $expectedValue" in {
        d.pretty should ===(expectedValue)
      }
  }

  it should "work with infinity" in {
    Duration.Inf.pretty should include("infinity")
  }

  it should "work with -infinity" in {
    Duration.MinusInf.pretty should include("minus infinity")
  }

  it should "work with undefined" in {
    Duration.Undefined.pretty should include("undefined")
  }
}
