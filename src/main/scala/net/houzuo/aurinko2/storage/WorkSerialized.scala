package net.houzuo.aurinko2.storage

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

import scala.concurrent.Promise
import scala.concurrent.promise

/** A class with a mutable value. */
case class Output[T](var data: T)

/**
 * Serialized work loads. Guaranteed fairness.
 */
trait WorkSerialized[I] {
  private val queue = new LinkedBlockingQueue[Tuple2[I, Promise[I]]]

  /** Add an item to the work queue. */
  def offer(work: I): Promise[I] = {
    val p = promise[I]
    queue.put((work, p))
    return p
  }

  /** Return length of the work queue. */
  def queueLength = queue.size

  /** Start working. */
  new Thread {
    override def run() {
      while (true) {
        val (work, result) = queue.poll(Long.MaxValue, TimeUnit.DAYS)
        workOn(work, result)
      }
    }
  }.start()

  protected def workOn(work: I, result: Promise[I])
}
