import org.scalatest.FunSuite

import java.io.File
import aurinko2.logic.Collection

class CollectionTest extends FunSuite {
  test("Insert and read from collection") {
    val tmpFile = File.createTempFile("Aurinko2", System.nanoTime().toString)
    tmpFile.deleteOnExit()
    val col = new Collection(tmpFile.getAbsolutePath())
    val pos = Seq(col.insert(<root>1</root>, true),
      col.insert(<root>2</root>, true),
      col.insert(<root>3</root>, true))
    assert(col.read(pos(0)) == <root>1</root>)
    assert(col.read(pos(1)) == <root>2</root>)
    assert(col.read(pos(2)) == <root>3</root>)
  }
}