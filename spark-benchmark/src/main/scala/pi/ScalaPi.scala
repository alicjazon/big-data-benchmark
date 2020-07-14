package pi

import scala.math.random

/** Computes an approximation to pi */
object ScalaPi extends App {
  val n = if (args.length > 1) args(1).toInt else 10000000
  val t0 = System.nanoTime()

  //*** Scala style
//  val count = (1 until n).map { _ =>
//    val x = random * 2 - 1
//    val y = random * 2 - 1
//    if (x * x + y * y <= 1) 1 else 0
//  }.reduce(_ + _) * 4.0 / (n - 1)
//  println(s"*****Pi is $count *****")

  //*** Java style
  var count: Int = 0
  var i: Int = 1
  while (i <= n) {
    val x = random * 2 - 1
    val y = random * 2 - 1
    if (x * x + y * y <= 1) count += 1
    i += 1
  }
  val pi: Double = 4.0 * count / (n - 1)
  System.out.println("Pi is: " + pi)

  val t1 = System.nanoTime()
  println("Elapsed time: " + (t1 - t0) + " ns")
}
