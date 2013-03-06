package aurinko2.test

object TimedExecution {

  def time(times: Int)(func: => Unit) {
    val start = System.currentTimeMillis()
    for (i <- 1 to times)
      func
    val duration = (System.currentTimeMillis() - start).toDouble
    println(s"${duration / times} ms per iteration; ${times / duration * 1000} iterations per second; $duration ms in total.")
  }

  def average(iterations: Int)(func: => Unit) {
    val start = System.currentTimeMillis()
    func
    val duration = (System.currentTimeMillis() - start).toDouble
    println(s"${duration / iterations} ms per iteration; ${iterations / duration * 1000} iterations per second; $duration ms in total.")
  }
}