package storage

import java.io.File
import java.io.RandomAccessFile

import org.scalatest.FunSuite

import aurinko2.storage.Log
import test.TemporaryFactory.log

class LogTest extends FunSuite {
  val entry = """
The standard source locations for testing are:
Scala sources in src/test/scala/
Java sources in src/test/java/
Resources for the test classpath in src/test/resources/

The resources may be accessed from tests by using the getResource methods of java.lang.Class or java.lang.ClassLoader.

The main Scala testing frameworks (specs2, ScalaCheck, and ScalaTest) provide an implementation of the common test interface and only need to be added to the classpath to work with sbt. For example, ScalaCheck may be used by declaring it as a managed dependency:
    libraryDependencies += "org.scala-tools.testing" %% "scalacheck" % "1.9" % "test"
""".trim()
  val entryBytes = entry.getBytes()

  test("insert and read") {
    val lo = log
    for (i <- 0 to 100000)
      assert(new String(lo.read(lo.insert(entryBytes))).trim().equals(entry))
  }

  test("read log entry given incorrect ID") {
    val lo = log
    for (i <- 0 to 1000)
      lo.insert(entryBytes)
    intercept[IllegalArgumentException] {
      lo.read(1000000000) // 1G
    }
  }

  test("iterate all entries") {
    val lo = log
    for (i <- 0 until 100000)
      lo.insert(entryBytes)

    def allEqual(): Boolean = {
      lo.foreach { e =>
        if (!new String(e).trim().equals(entry))
          return false
      }
      return true
    }

    assert(allEqual())
  }
}