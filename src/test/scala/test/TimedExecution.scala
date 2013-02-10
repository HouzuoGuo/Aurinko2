package test

object TimedExecution {
  def time(times: Int)(func: => Unit) {
    val start = System.currentTimeMillis()
    for (i <- 1 to times)
      func
    val duration = (System.currentTimeMillis() - start).toDouble
    println(s"${duration / times} ms per iteration; ${times / duration * 1000} iterations per second; $duration ms in total.")
  }
}