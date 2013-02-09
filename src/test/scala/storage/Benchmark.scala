package storage

import org.scalatest.FunSuite
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import java.io.RandomAccessFile
import java.io.File
import aurinko2.storage.Collection

class Benchmark extends FunSuite {
  def time(times: Int)(func: () => Unit) {
    val start = System.currentTimeMillis()
    for (i <- 1 to times) {
      func()
    }
    val end = System.currentTimeMillis()
    println(s"Total: ${end - start} ms")
    println(s"Per iteration: ${(end - start) / times.toDouble } ms")
  }

  val docBytes = """
The standard source locations for testing are:
Scala sources in src/test/scala/
Java sources in src/test/java/
Resources for the test classpath in src/test/resources/

The resources may be accessed from tests by using the getResource methods of java.lang.Class or java.lang.ClassLoader.
""".getBytes()

  test("storage layer performance benchmark") {
    val tmp = File.createTempFile(System.nanoTime().toString, "Aurinko2")
    tmp.deleteOnExit()
    val raf = new RandomAccessFile(tmp, "rw")
    val col = new Collection(raf.getChannel())
    val random = new Random()
    val positions = new ArrayBuffer[Int]
    println("Insert .5m documents")
    time(500000)(() => {
      positions += col.insert(docBytes)
    })
    println("Read .5m documents")
    time(500000)(() => {
      col.read(positions(random.nextInt(500000)))
    })
  }
}
