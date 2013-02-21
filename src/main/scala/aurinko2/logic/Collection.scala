package aurinko2.logic

import java.io.RandomAccessFile
import scala.collection.mutable.HashMap
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.xml.Elem
import scala.xml.XML.loadString
import aurinko2.storage.{ Collection => CollFile }
import aurinko2.storage.CollectionInsert
import aurinko2.storage.Hash
import aurinko2.storage.Output
import aurinko2.storage.CollectionRead

class Collection(val path: String) {

  // Open collection
  val hash = new HashMap[Seq[String], Hash]
  val collection = new CollFile(new RandomAccessFile(path, "rw").getChannel())

  // Index management
  def index(path: Seq[String]) {
  }

  def unindex(path: Seq[String]) {

  }

  // Document management
  def read(id: Int): Elem = {
    val work = CollectionRead(id, new Output[Array[Byte]](null))
    Await.result(collection.offer(work).future, Long.MaxValue second)
    if (work.data.data == null)
      return null
    return loadString(new String(work.data.data))
  }

  def insert(doc: Elem, wait: Boolean = false): Int = {
    val work = CollectionInsert(doc.toString.getBytes(), new Output[Int](0))
    val p = collection.offer(work)
    if (!wait)
      return -1

    Await.result(p.future, Long.MaxValue second)
    return work.pos.data
  }

  def update(id: Int, doc: Elem, wait: Boolean = false): Int = {
    0
  }

  def delete(id: Int, wait: Boolean = false) = {

  }
}