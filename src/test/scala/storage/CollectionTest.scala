package storage

import org.scalatest.FunSuite

import test.TemporaryFactory.collection

class CollectionTest extends FunSuite {
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

  test("insert and read") {
    val col = collection

    for (i <- 0 to 100)
      assert(new String(col.read(col.insert(docBytes))).trim().equals(doc))
  }

  test("update and read") {
    val col = collection
    val docs = Array(col.insert("1".getBytes()), col.insert("2".getBytes()),
      col.insert("A".getBytes()), col.insert("B".getBytes()))
    assert(new String(col.read(col.update(docs(0), "A".getBytes()))).trim().equals("A"))
    assert(new String(col.read(col.update(docs(1), "BC".getBytes()))).trim().equals("BC"))
    assert(new String(col.read(col.update(docs(2), "DEF".getBytes()))).trim().equals("DEF"))

    // Re-insert
    assert(new String(col.read(col.update(docs(3), "GHIJKL".getBytes()))).trim().equals("GHIJKL"))
  }

  test("delete and read") {
    val col = collection
    val docs = Array(col.insert("1".getBytes()), col.insert("2".getBytes()),
      col.insert("A".getBytes()), col.insert("B".getBytes()))
    col.delete(docs(0))
    col.delete(docs(3))
    assert(col.read(docs(0)) == null)
    assert(col.read(docs(3)) == null)
    assert(new String(col.read(docs(1))).trim().equals("2"))
    assert(new String(col.read(docs(2))).trim().equals("A"))
  }

  test("read/insert/update/delete given incorrect ID") {
    val col = collection
    col.insert("1".getBytes())
    col.insert("2".getBytes())
    col.insert("A".getBytes())
    col.insert("B".getBytes())
    intercept[IllegalArgumentException] {
      col.delete(1000000000)
    }
    intercept[IllegalArgumentException] {
      col.update(1000000000, "".getBytes())
    }
    intercept[IllegalArgumentException] {
      col.update(13, "".getBytes())
    }
    intercept[IllegalArgumentException] {
      col.read(1000000000)
    }
    intercept[IllegalArgumentException] {
      col.read(13)
    }
  }
}