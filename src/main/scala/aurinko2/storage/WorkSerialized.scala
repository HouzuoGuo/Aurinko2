package aurinko2.storage

import java.util.concurrent.{ TimeUnit, LinkedBlockingQueue }
import scala.concurrent.{ promise, Promise }

case class Output[T](var data: T)

trait WorkSerialized[I] {
  val queue = new LinkedBlockingQueue[Tuple2[I, Promise[I]]]

  def offer(work: I): Promise[I] = {
    val p = promise[I]
    queue.put((work, p))
    return p
  }

  def start() {
    while (true) {
      val (work, result) = queue.poll(Long.MaxValue, TimeUnit.DAYS)
      workOn(work, result)
    }
  }

  def workOn(work: I, result: Promise[I])
}