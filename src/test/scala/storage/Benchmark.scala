package storage

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.scalatest.FunSuite

import test.TemporaryFactory.{ collection, hashTable, log }
import test.TimedExecution.time

class Benchmark extends FunSuite {
  val random = new Random()
  val docBytes = """
The standard source locations for testing are:
Scala sources in src/test/scala/
Java sources in src/test/java/
Resources for the test classpath in src/test/resources/

The resources may be accessed from tests by using the getResource methods of java.lang.Class or java.lang.ClassLoader.
""".getBytes()

  test("hash table storage layer performance benchmark") {
    val entries = new ArrayBuffer[Int]
    val iterations = 2000000
    val hash = hashTable(12, 100)

    println("Put 200k entries")
    time(iterations) {
      val number = random.nextInt(iterations).hashCode
      entries += number
      hash.put(number, number)
    }
    println("Get 200k entries")
    time(iterations) { hash.get(entries(random.nextInt(iterations)), 1, (_1: Int, _2: Int) => { true }) }
    println("Delete 200k entries")
    time(iterations) { hash.remove(entries(random.nextInt(iterations)), 1, (_1: Int, _2: Int) => { true }) }
  }

  test("collection storage layer performance benchmark") {
    val positions = new ArrayBuffer[Int]
    val iterations = 200000
    val col = collection

    println("Insert 200k documents")
    time(iterations) { positions += col.insert(docBytes) }
    println("Read 200k documents")
    time(iterations) { col.read(positions(random.nextInt(iterations))) }
    println("Update 200k documents")
    time(iterations) { col.update(positions(random.nextInt(iterations)), docBytes) }
    println("Delete 200k documents")
    time(iterations) { col.delete(positions(random.nextInt(iterations))) }
  }

  test("log storage layer performance benchmark") {
    val positions = new ArrayBuffer[Int]
    val iterations = 200000
    val lo = log

    println("Insert 200k log entries")
    time(iterations) { positions += lo.insert(docBytes) }
    println("Read 200k log entries")
    time(iterations) { lo.read(positions(random.nextInt(iterations))) }
  }
}
