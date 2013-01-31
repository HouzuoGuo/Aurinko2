import org.scalatest.FunSuite
import aurinko2.storage.Log
import java.io.RandomAccessFile
import java.io.File

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

  def tmpLog(): Log = {
    val tmp = File.createTempFile(System.nanoTime().toString, "Aurinko2")
    tmp.deleteOnExit()
    val raf = new RandomAccessFile(tmp, "rw")
    return new Log(raf.getChannel())
  }

  test("insert and read") {
    val log = tmpLog()
    for (i <- 0 to 100000)
      assert(new String(log.read(log.insert(entryBytes))).trim().equals(entry))
  }

  test("read log entry given incorrect ID") {
    val log = tmpLog()
    for (i <- 0 to 1000)
      log.insert(entryBytes)
    intercept[IllegalArgumentException] {
      log.read(1000000000) // 1G
    }
  }

  test("iterate all entries") {
    val log = tmpLog()
    for (i <- 0 until 100000)
      log.insert(entryBytes)

    def allEqual(): Boolean = {
      log.foreach { e =>
        if (!new String(e).trim().equals(entry))
          return false
      }
      return true
    }

    assert(allEqual())
  }
}