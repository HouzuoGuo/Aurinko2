package aurinko2.test.logic

import org.scalatest.FunSuite

import aurinko2.logic.Collection
import TemporaryFactory.collection

class CollectionTest extends FunSuite {
  test("Collection document CRUD without index") {
    val col = collection

    // Insert & read
    val pos = Seq(col.insert(<root>1</root>, true),
      col.insert(<root>2</root>, true),
      col.insert(<root>3</root>, true))
    assert(col.read(pos(0)) == <root>1</root>)
    assert(col.read(pos(1)) == <root>2</root>)
    assert(col.read(pos(2)) == <root>3</root>)

    // Update
    assert(col.read(col.update(pos(0), <root>a</root>, true)) == <root>a</root>)
    assert(col.read(col.update(pos(1), <root>b</root>, true)) == <root>b</root>)

    // Delete
    col.delete(pos(2), true)
    assert(col.read(pos(2)) == null)
  }
}