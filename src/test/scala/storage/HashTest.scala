package storage

import org.scalatest.FunSuite

import test.TemporaryFactory.hashTable

class HashTest extends FunSuite {

  test("put and get (with bucket grow)") {
    val hash = hashTable(2, 2)
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

  test("remove then get") {
    val hash = hashTable(2, 2)
    hash.put("A".hashCode(), 1)
    hash.put("B".hashCode(), 2)
    hash.put("C".hashCode(), 3)
    hash.put("D".hashCode(), 4)
    hash.put("E".hashCode(), 5)
    hash.put("F".hashCode(), 6)
    hash.put("A".hashCode(), 7)
    hash.put("B".hashCode(), 8)
    hash.put("B".hashCode(), 9)

    hash.remove("A".hashCode(), -1, (i1: Int, i2: Int) => { i2 > 5 })
    hash.remove("B".hashCode(), 2, (i1: Int, i2: Int) => { true })

    val a = hash.get("A".hashCode(), -1, (_: Int, _2: Int) => { true }).map { _._2 }
    val b = hash.get("B".hashCode(), -1, (_: Int, _2: Int) => { true }).map { _._2 }
    val c = hash.get("C".hashCode(), -1, (_: Int, _2: Int) => { true }).map { _._2 }
    val d = hash.get("D".hashCode(), -1, (_: Int, _2: Int) => { true }).map { _._2 }
    val e = hash.get("E".hashCode(), -1, (_: Int, _2: Int) => { true }).map { _._2 }
    val f = hash.get("F".hashCode(), -1, (_: Int, _2: Int) => { true }).map { _._2 }

    assert(a.sameElements(List(1)))
    assert(b.sameElements(List(9)))
    assert(c.sameElements(List(3)))
    assert(d.sameElements(List(4)))
    assert(e.sameElements(List(5)))
    assert(f.sameElements(List(6)))
  }
}
