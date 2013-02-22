package aurinko2.test.storage

import java.io.File
import java.io.RandomAccessFile

import org.scalatest.FunSuite

import aurinko2.storage.Collection

class AppendFileTest extends FunSuite {

  val doc = """
The standard source locations for testing are:
Scala sources in src/test/scala/
Java sources in src/test/java/
Resources for the test classpath in src/test/resources/

The resources may be accessed from tests by using the getResource methods of java.lang.Class or java.lang.ClassLoader.

The main Scala testing frameworks (specs2, ScalaCheck, and ScalaTest) provide an implementation of the common test interface and only need to be added to the classpath to work with sbt. For example, ScalaCheck may be used by declaring it as a managed dependency:
    libraryDependencies += "org.scala-tools.testing" %% "scalacheck" % "1.9" % "test"
""".trim()
  val docBytes = doc.getBytes()

  test("re-open file and find next append position") {
    val tmp = File.createTempFile(System.nanoTime().toString, "Aurinko2")
    tmp.deleteOnExit()
    val raf = new RandomAccessFile(tmp, "rw")
    val col = new Collection(raf.getChannel())

    // The collection will have to grow a few times
    for (i <- 0 to 100000)
      assert(new String(col.read(col.insert(docBytes))).trim().equals(doc))
    col.close()

    // Re-open it and test insert/read
    val reopenedRaf = new RandomAccessFile(tmp, "rw")
    val reopened = new Collection(reopenedRaf.getChannel())
    for (i <- 0 to 100) {
      assert(new String(reopened.read(reopened.insert(docBytes))).trim().equals(doc))
    }
  }
}