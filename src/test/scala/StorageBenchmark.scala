import org.scalatest.FunSuite

class StorageBenchmark extends FunSuite {
  def time(times: Int)(func: => Unit) {
    def start = System.nanoTime()
    for (i <- 1 to times)
      func
    def end = System.nanoTime()
  }
}