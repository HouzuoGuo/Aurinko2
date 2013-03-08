package aurinko2.test.logic

import scala.collection.mutable.ListBuffer
import scala.xml.Elem
import scala.xml.XML.loadString

import org.scalatest.FunSuite

import TemporaryFactory.collection
import aurinko2.logic.Collection

class CollectionTest extends FunSuite {

  test("collection document CRUD without index") {
    val col = collection

    // Insert & read
    val pos = Seq(col.insert(<root>1</root>),
      col.insert(<root>2</root>),
      col.insert(<root>3</root>))
    assert(col.read(pos(0)).get == <root>1</root>)
    assert(col.read(pos(1)).get == <root>2</root>)
    assert(col.read(pos(2)).get == <root>3</root>)

    // Update
    assert(col.read(col.update(pos(0), <root>a</root>).get).get == <root>a</root>)
    assert(col.read(col.update(pos(1), <root>b</root>).get).get == <root>b</root>)

    // Delete
    col.delete(pos(2))
    assert(col.read(pos(2)).isEmpty)

    // Update at invalid ID
    intercept[IllegalArgumentException] {
      col.update(1000000000, <abc></abc>)
    }

    // Delete at invalid ID
    intercept[IllegalArgumentException] {
      col.delete(1000000000)
    }
  }

  test("collection index insert/update/delete") {
    val col = collection
    col.index(List("a"), 4, 4)
    col.index(List("a", "b", "c"), 4, 4)
    val docs = Seq(col.insert(<root><a>1</a><a>2</a></root>),
      col.insert(<root><a>3</a><a>4</a></root>),
      col.insert(<root><a><ha/></a></root>))
    val aIndex = col.hashes(List("a"))
    val anotherIndex = col.hashes(List("a", "b", "c"))

    assert(aIndex != null && anotherIndex != null)

    // Insert to index
    (() => {
      val v1 = aIndex._2.get("1".hashCode(), -1, (_, _2) => true).map { _._2 }
      val v2 = aIndex._2.get("2".hashCode(), -1, (_, _2) => true).map { _._2 }
      val v3 = aIndex._2.get("3".hashCode(), -1, (_, _2) => true).map { _._2 }
      val v4 = aIndex._2.get("4".hashCode(), -1, (_, _2) => true).map { _._2 }
      val element = aIndex._2.get(<a><ha/></a>.toString.hashCode(), -1, (_, _2) => true).map { _._2 }
      assert(v1 == List(docs(0)))
      assert(v2 == List(docs(0)))
      assert(v3 == List(docs(1)))
      assert(v4 == List(docs(1)))
      assert(element == List(docs(2)))
    })()

    // Update indexed document
    val updated = Seq(col.update(docs(0), <root><a>5</a><a>6</a></root>))
    (() => {
      val v1 = aIndex._2.get("1".hashCode(), -1, (_, _2) => true).map { _._2 }
      val v2 = aIndex._2.get("2".hashCode(), -1, (_, _2) => true).map { _._2 }
      val v3 = aIndex._2.get("3".hashCode(), -1, (_, _2) => true).map { _._2 }
      val v4 = aIndex._2.get("4".hashCode(), -1, (_, _2) => true).map { _._2 }
      val v5 = aIndex._2.get("5".hashCode(), -1, (_, _2) => true).map { _._2 }
      val v6 = aIndex._2.get("6".hashCode(), -1, (_, _2) => true).map { _._2 }
      val element = aIndex._2.get(<a><ha/></a>.toString.hashCode(), -1, (_, _2) => true).map { _._2 }

      // Old indexed value disappears
      assert(v1 == List())
      assert(v2 == List())

      // Existing ones not affected
      assert(v3 == List(docs(1)))
      assert(v4 == List(docs(1)))
      assert(element == List(docs(2)))

      // Updated document updates indexed values
      assert(v5 == List(docs(0)))
      assert(v6 == List(docs(0)))
    })()

    // Delete indexed document
    col.delete(docs(1))
    (() => {
      val v1 = aIndex._2.get("1".hashCode(), -1, (_, _2) => true).map { _._2 }
      val v2 = aIndex._2.get("2".hashCode(), -1, (_, _2) => true).map { _._2 }
      val v3 = aIndex._2.get("3".hashCode(), -1, (_, _2) => true).map { _._2 }
      val v4 = aIndex._2.get("4".hashCode(), -1, (_, _2) => true).map { _._2 }
      val v5 = aIndex._2.get("5".hashCode(), -1, (_, _2) => true).map { _._2 }
      val v6 = aIndex._2.get("6".hashCode(), -1, (_, _2) => true).map { _._2 }
      val element = aIndex._2.get(<a><ha/></a>.toString.hashCode(), -1, (_, _2) => true).map { _._2 }

      // Existing ones not affected
      assert(v1 == List())
      assert(v2 == List())
      assert(element == List(docs(2)))
      assert(v5 == List(docs(0)))
      assert(v6 == List(docs(0)))

      // Deleted document deletes indexed values
      assert(v3 == List())
      assert(v4 == List())
    })()
  }

  test("save/load collection configuration") {
    // Create collection with two indexes
    val col = collection
    col.index(List("a", "b"), 3, 4)
    col.index(List("c", "d", "e"), 5, 6)
    col.close()
    // Re-open it
    val reopen = new Collection(col.path)
    assert(reopen.hashes.keySet == Set(List("a", "b"), List("c", "d", "e")))
    val indexParams = reopen.hashes.map(_._2._2).toSeq
    assert(indexParams(0).hashBits == 3)
    assert(indexParams(0).perBucket == 4)
    assert(indexParams(1).hashBits == 5)
    assert(indexParams(1).perBucket == 6)
  }

  test("iterate all document IDs") {
    val col = collection
    val docs = Array(col.insert(<a>1</a>), col.insert(<b>2</b>), col.insert(<c>3</c>))
    docs(2) = col.update(docs(2), <d>asdjfkljsadkfajsdlkjfaklsjfklajkjfskjksdfj</d>).get
    col.delete(docs(1))
    assert(col.all == List(docs(0), docs(2)))
  }

  test("create and delete index in non-empty collection") {
    val col = collection
    val docs = Seq(col.insert(<root><a>1</a><a>2</a></root>),
      col.insert(<root><a>3</a><a>4</a></root>),
      col.insert(<root><a><ha/></a></root>))

    col.index(List("a"), 4, 4)
    col.index(List("a", "b", "c"), 4, 4)
    val aIndex = col.hashes(List("a"))
    val anotherIndex = col.hashes(List("a", "b", "c"))
    assert(aIndex != null && anotherIndex != null)

    val v1 = aIndex._2.get("1".hashCode(), -1, (_, _2) => true).map { _._2 }
    val v2 = aIndex._2.get("2".hashCode(), -1, (_, _2) => true).map { _._2 }
    val v3 = aIndex._2.get("3".hashCode(), -1, (_, _2) => true).map { _._2 }
    val v4 = aIndex._2.get("4".hashCode(), -1, (_, _2) => true).map { _._2 }
    val element = aIndex._2.get(<a><ha/></a>.toString.hashCode(), -1, (_, _2) => true).map { _._2 }
    assert(v1 == List(docs(0)))
    assert(v2 == List(docs(0)))
    assert(v3 == List(docs(1)))
    assert(v4 == List(docs(1)))
    assert(element == List(docs(2)))

    col.unindex(List("a"))
    assert(col.hashes.size == 1)
    intercept[NoSuchElementException] {
      col.hashes(List("a"))
    }
  }

  test("flush and close collection") {
    val col = collection
    col.save()
    col.close()
  }

  test("get into an XML document") {
    val test = <root>
                 <a>1</a>
                 <a whatever="a">2</a>
                 <b>
                   <c>c</c>
                 </b>
               </root>
    assert(Collection.getIn(test, List("a")) == List("1", "2"))
    assert(Collection.getIn(test, List("b", "c")) == List("c"))
    assert(Collection.getIn(test, List("z")) == List())
  }

}