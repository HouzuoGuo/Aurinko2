package aurinko2.test.logic

import scala.collection.mutable.ArrayBuffer
import scala.math.max
import scala.util.Random
import scala.xml.Elem

import org.scalatest.FunSuite

import TemporaryFactory.collection
import aurinko2.test.TimedExecution.average
import aurinko2.test.TimedExecution.time

class Benchmark extends FunSuite {

  val random = new Random()

  /*
   * This benchmark spawns multiple threads to simulate concurrent read and write operations.
   */
  test("collection storage layer performance benchmark") {
    val numThreads = max(Runtime.getRuntime().availableProcessors(), 4)
    val iterations = 200000
    val positions = new ArrayBuffer[Int](iterations)
    val col = collection
    col.index(List("a", "b", "c"), 14, 200)
    col.index(List("d", "e", "f"), 14, 200)

    println("Insert 200k documents with 2 indexes")
    val inserts = for (i <- 1 to numThreads) yield new Thread {
      override def run() {
        for (j <- 1 to iterations / numThreads) {
          val position = col.insert(
            <root>
              <a><b><c>{ random.nextInt(20000) }</c></b></a>
              <d><e><f>{ random.nextInt(20000) }</f><f>{ random.nextInt(20000) }</f></e></d>
              <pudding>
                The standard source locations for testing are:
Scala sources in src/test/scala/
Java sources in src/test/java/
Resources for the test classpath in src/test/resources/

The resources may be accessed from tests by using the getResource methods of java.lang.Class or java.lang.ClassLoader.
              </pudding>
            </root>)
          positions.synchronized { positions += position }
        }
      }
    }
    average(iterations) {
      inserts foreach { _.start() }
      inserts foreach { _.join() }
    }

    println("Read 200k documents")
    val reads = for (i <- 1 to numThreads) yield new Thread {
      override def run() {
        for (j <- 1 to iterations / numThreads)
          col.read(positions(random.nextInt(iterations)))
      }
    }
    average(iterations) {
      reads foreach { _.start() }
      reads foreach { _.join() }
    }

    println("Update 200k documents")
    val updates = for (i <- 1 to numThreads) yield new Thread {
      override def run() {
        for (j <- 1 to iterations / numThreads)
          col.update(positions(random.nextInt(iterations)),
            <root>
              <a><b><c>{ random.nextInt(20000) }</c></b></a>
              <d><e><f>{ random.nextInt(20000) }</f><f>{ random.nextInt(20000) }</f></e></d>
              <pudding>
                The standard source locations for testing are:
Scala sources in src/test/scala/
Java sources in src/test/java/
Resources for the test classpath in src/test/resources/
The resources may be accessed from tests by using the getResource methods of java.lang.Class or java.lang.ClassLoader.
              </pudding>
            </root>)
      }
    }
    average(iterations) {
      updates foreach { _.start() }
      updates foreach { _.join() }
    }

    println("Delete 200k documents")
    val deletes = for (i <- 1 to numThreads) yield new Thread {
      override def run() {
        for (j <- 1 to iterations / numThreads)
          col.delete(positions(random.nextInt(iterations)))
      }
    }
    average(iterations) {
      deletes foreach { _.start() }
      deletes foreach { _.join() }
    }

    println("Get all 200k document IDs")
    time(1) { col.all }

    println("Index 200k documents")
    col.unindex(List("a", "b", "c"))
    time(1) { col.index(List("a", "b", "c"), 14, 200) }
  }
}
