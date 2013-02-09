package storage

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.scalatest.FunSuite

import test.TemporaryFactory.collection
import test.TemporaryFactory.hashTable
import test.TimedExecution.time

class Benchmark extends FunSuite {

  val docBytes = """
The standard source locations for testing are:
Scala sources in src/test/scala/
Java sources in src/test/java/
Resources for the test classpath in src/test/resources/

The resources may be accessed from tests by using the getResource methods of java.lang.Class or java.lang.ClassLoader.
""".getBytes()

  test("hash table storage layer performance benchmark") {
    val random = new Random()
    val entries = new ArrayBuffer[Int]
    val warmUpIterations = 20000
    val iterations = 300000

    println("Warming up")
    val warmup = hashTable(12, 100)
    for (i <- 1 to warmUpIterations) {
      val number = random.nextInt(warmUpIterations).hashCode
      entries += number
      warmup.put(number, number)
    }
    for (number <- entries)
      warmup.get(number, 1, (_1: Int, _2: Int) => { true })
    for (number <- entries)
      warmup.remove(number, 1, (_1: Int, _2: Int) => { true })
    entries.clear()

    val hash = hashTable(14, 100)
    println("Put 300k entries")
    time(iterations) {
      val number = random.nextInt(warmUpIterations).hashCode
      entries += number
      hash.put(number, number)
    }
    println("Get 300k entries")
    time(iterations) { hash.get(entries(random.nextInt(iterations)), 1, (_1: Int, _2: Int) => { true }) }
    println("Delete 300k entries")
    time(iterations) { hash.remove(entries(random.nextInt(iterations)), 1, (_1: Int, _2: Int) => { true }) }
  }

  test("collection storage layer performance benchmark") {
    val random = new Random()
    val positions = new ArrayBuffer[Int]
    val warmUpIterations = 20000
    val iterations = 300000

    println("Warming up")
    val warmup = collection
    for (i <- 1 to warmUpIterations)
      positions += warmup.insert(docBytes)
    for (i <- 1 to warmUpIterations)
      warmup.read(positions(random.nextInt(warmUpIterations)))
    for (i <- 1 to warmUpIterations)
      warmup.update(positions(random.nextInt(warmUpIterations)), docBytes)
    for (i <- 1 to warmUpIterations)
      warmup.delete(positions(random.nextInt(warmUpIterations)))
    positions.clear()

    val col = collection
    println("Insert 300k documents")
    time(iterations) { positions += col.insert(docBytes) }
    println("Read 300k documents")
    time(iterations) { col.read(positions(random.nextInt(iterations))) }
    println("Update 300k documents")
    time(iterations) { col.update(positions(random.nextInt(iterations)), docBytes) }
    println("Delete 300k documents")
    time(iterations) { col.delete(positions(random.nextInt(iterations))) }
  }

  test("log storage layer performance benchmark") {

  }
}
