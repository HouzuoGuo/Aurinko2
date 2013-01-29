import org.scalatest.FunSuite
import java.io.File
import java.io.RandomAccessFile
import aurinko2.storage.Collection

class CollectionTest extends FunSuite {
  val largeDoc = """
The standard source locations for testing are:
Scala sources in src/test/scala/
Java sources in src/test/java/
Resources for the test classpath in src/test/resources/

The resources may be accessed from tests by using the getResource methods of java.lang.Class or java.lang.ClassLoader.

The main Scala testing frameworks (specs2, ScalaCheck, and ScalaTest) provide an implementation of the common test interface and only need to be added to the classpath to work with sbt. For example, ScalaCheck may be used by declaring it as a managed dependency:
    libraryDependencies += "org.scala-tools.testing" %% "scalacheck" % "1.9" % "test"

The fourth component "test" is the configuration and means that ScalaCheck will only be on the test classpath and it isn't needed by the main sources. This is generally good practice for libraries because your users don't typically need your test dependencies to use your library.

With the library dependency defined, you can then add test sources in the locations listed above and compile and run tests. The tasks for running tests are test and test-only. The test task accepts no command line arguments and runs all tests:
> test
"""
  val largeDocBytes = largeDoc.getBytes()

  test("insert and read") {
    val tmp = File.createTempFile(System.nanoTime().toString, null)
    tmp.deleteOnExit()
    val raf = new RandomAccessFile(tmp, "rw")
    val col = new Collection(raf.getChannel())
    for (i <- 0 to 100000) {
      // The document has to be large enough to make collection grow
      assert(new String(col.read(col.insert(largeDocBytes))).trim().equals(largeDoc))
    }
  }
}