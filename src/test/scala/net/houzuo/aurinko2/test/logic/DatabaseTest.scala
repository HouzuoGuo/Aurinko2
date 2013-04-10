package net.houzuo.aurinko2.test.logic

import org.scalatest.FunSuite
import net.houzuo.aurinko2.test.logic.TemporaryFactory.database
import net.houzuo.aurinko2.logic.Database

class DatabaseTest extends FunSuite {
  test("open database, CRUD collections") {
    val db = database

    // Create collection
    db.create("a")
    db.create("b")
    intercept[Exception] {
      db.create("a")
    }
    assert(db.get("a") != null)
    assert(db.get("b") != null)

    // Get all collection names
    assert(db.all == Set("a", "b"))

    // Rename collection
    db.rename("b", "c")
    intercept[Exception] {
      db.get("b")
    }
    assert(db.get("c") != null)
    intercept[Exception] {
      db.rename("c", "a")
    }

    // Delete collection
    intercept[Exception] {
      db.drop("does not exist")
    }
    db.drop("a")
    intercept[Exception] {
      db.get("a")
    }
    assert(db.get("c") != null)
  }

  test("repair collection") {
    val db = database
    db.create("toRepair")
    val col = db.get("toRepair")
    col.index(List("a", "b", "c"), 10, 10)
    col.index(List("a", "b", "d"), 10, 10)
    val left = col.update(col.insert(<root>1</root>), <root>2</root>)
    col.delete(col.insert(<root>3</root>))
    db.repair("toRepair")
    val repaired = db.get("toRepair")
    assert(repaired.all().size == 1)
    assert(repaired.hashes.keySet.toSet == Set(List("a", "b", "c"), List("a", "b", "d")))
    assert(repaired.read(repaired.all()(0)).get == <root>2</root>)
  }

  test("close then open database") {
    val db = database
    db.create("a")
    db.get("a").index(List("a"), 10, 10)
    db.create("b")
    db.save()
    db.close()
    val reopened = new Database(db.path)
    assert(reopened.all.size == 2)
    assert(reopened.get("a") != null)
    assert(reopened.get("b") != null)
  }
}