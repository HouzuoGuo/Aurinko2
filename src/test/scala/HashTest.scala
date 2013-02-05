import org.scalatest.FunSuite
import aurinko2.storage.Hash
import java.io.RandomAccessFile
import java.io.File

class HashTest extends FunSuite {
  def tmpHash(): Hash = {
    val tmp = File.createTempFile(System.nanoTime().toString, "Aurinko2")
    tmp.deleteOnExit()
    val raf = new RandomAccessFile(tmp, "rw")
    return new Hash(raf.getChannel(), 2, 2)
  }

  test("put and get (including grow)") {
    val hash = tmpHash()
    hash.put("A".hashCode(), 1)
    hash.put("B".hashCode(), 2)
    hash.put("C".hashCode(), 3)
    hash.put("D".hashCode(), 4)
    hash.put("E".hashCode(), 5)
    hash.put("F".hashCode(), 6)
    hash.put("A".hashCode(), 7)
    hash.put("B".hashCode(), 8)
    hash.put("B".hashCode(), 9)

    val a = hash.get("A".hashCode(), -1, (_: Int, _2: Int) => { true }).map { _._2 }
    val b = hash.get("B".hashCode(), -1, (_: Int, _2: Int) => { true }).map { _._2 }
    val c = hash.get("C".hashCode(), -1, (_: Int, _2: Int) => { true }).map { _._2 }
    val d = hash.get("D".hashCode(), -1, (_: Int, _2: Int) => { true }).map { _._2 }
    val e = hash.get("E".hashCode(), -1, (_: Int, _2: Int) => { true }).map { _._2 }
    val f = hash.get("F".hashCode(), -1, (_: Int, _2: Int) => { true }).map { _._2 }

    assert(a.sameElements(List(1, 7)))
    assert(b.sameElements(List(2, 8, 9)))
    assert(c.sameElements(List(3)))
    assert(d.sameElements(List(4)))
    assert(e.sameElements(List(5)))
    assert(f.sameElements(List(6)))
  }
}