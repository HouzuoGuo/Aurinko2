package net.houzuo.aurinko2.test.storage

import org.scalatest.FunSuite

import TemporaryFactory.hashTable

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

    assert(hash.get("A".hashCode(), -1, (_: Int, _2: Int) => { true }).map { _._2 } == List(1, 7))
    assert(hash.get("B".hashCode(), -1, (_: Int, _2: Int) => { true }).map { _._2 } == List(2, 8, 9))
    assert(hash.get("C".hashCode(), -1, (_: Int, _2: Int) => { true }).map { _._2 } == List(3))
    assert(hash.get("D".hashCode(), -1, (_: Int, _2: Int) => { true }).map { _._2 } == List(4))
    assert(hash.get("E".hashCode(), -1, (_: Int, _2: Int) => { true }).map { _._2 } == List(5))
    assert(hash.get("F".hashCode(), -1, (_: Int, _2: Int) => { true }).map { _._2 } == List(6))
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

    assert(hash.get("A".hashCode(), -1, (_: Int, _2: Int) => { true }).map { _._2 } == List(1))
    assert(hash.get("B".hashCode(), -1, (_: Int, _2: Int) => { true }).map { _._2 } == List(9))
    assert(hash.get("C".hashCode(), -1, (_: Int, _2: Int) => { true }).map { _._2 } == List(3))
    assert(hash.get("D".hashCode(), -1, (_: Int, _2: Int) => { true }).map { _._2 } == List(4))
    assert(hash.get("E".hashCode(), -1, (_: Int, _2: Int) => { true }).map { _._2 } == List(5))
    assert(hash.get("F".hashCode(), -1, (_: Int, _2: Int) => { true }).map { _._2 } == List(6))
  }

  test("get all entries") {
    val hash = hashTable(2, 2)
    val entries = Map("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 4, "e" -> 5, "f" -> 6, "g" -> 7)
    entries foreach { entry => hash.put(entry._1.hashCode(), entry._2.hashCode) }
    assert(hash.allEntries.toMap == entries.map { entry => entry._1.hashCode -> entry._2 })
  }
}
