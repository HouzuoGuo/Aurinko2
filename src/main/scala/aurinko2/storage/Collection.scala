package aurinko2.storage

import java.nio.channels.FileChannel
import java.util.logging.Logger

import scala.concurrent.Promise

// Workloads
abstract class CollectionWork
case class CollectionInsert(doc: Array[Byte], pos: Output[Int]) extends CollectionWork
case class CollectionUpdate(id: Int, doc: Array[Byte], pos: Output[Int]) extends CollectionWork
case class CollectionDelete(id: Int) extends CollectionWork
case class CollectionRead(id: Int, data: Output[Array[Byte]]) extends CollectionWork
case class CollectionIterate(f: Array[Byte] => Unit) extends CollectionWork

object Collection {
  val LOG = Logger.getLogger(classOf[Hash].getName())

  /*
   * Collection file grows by 64MB when full.
   * Collection file is made of documents, each consists of:
   * - document header (int validity, int size of allocated room)
   * - document padding (left for the document to grow)
   */
  val GROWTH = 67108864
  val DOC_HEADER_SIZE = 8
  val DOC_PADDING = 2 // How much space (N times) to leave for a document to grow
  val DOC_VALID = 1
  val DOC_INVALID = 0
  val MAX_DOC_SIZE = 16777216 // Document may not be larger than 16 MB when inserted
  val MAX_DOC_ROOM = MAX_DOC_SIZE * (1 + DOC_PADDING)
}

class Collection(
  override protected val fc: FileChannel)
  extends AppendFile(
    fc,
    Collection.GROWTH,
    Collection.GROWTH)
  with WorkSerialized[CollectionWork] {

  /** Return document read at the position; return null if the document is no longer valid. */
  def read(id: Int): Array[Byte] = {
    if (id > appendAt)
      throw new IllegalArgumentException(s"Document $id does not exist")

    buf.position(id)
    val valid = buf.getInt()
    if (valid == Collection.DOC_INVALID)
      return null

    // Not a document header?
    if (valid != Collection.DOC_VALID)
      throw new IllegalArgumentException(s"Document $id does not exist")

    val room = buf.getInt()

    // Possible document header corruption, better repair the collection
    if (room > Collection.MAX_DOC_ROOM) {
      Collection.LOG.severe(s"Document $id has a header corruption - repair collection?")
      return null
    }
    val data = Array.ofDim[Byte](room)
    buf.get(data)
    return data
  }

  /** Insert a document; return inserted document ID. */
  def insert(doc: Array[Byte]): Int = {
    if (doc.length > Collection.MAX_DOC_SIZE)
      throw new IllegalArgumentException(s"Document is too large, it exceeds ${Collection.MAX_DOC_SIZE}")

    var id = -1
    val len = doc.length
    val room = len + len * Collection.DOC_PADDING
    val padding =
      id = appendAt
    checkGrow(room)
    buf.position(appendAt)
    buf.putInt(Collection.DOC_VALID)
    buf.putInt(room)
    buf.put(doc)
    buf.put(" ".*(len * Collection.DOC_PADDING).getBytes())
    appendAt += Collection.DOC_HEADER_SIZE + room
    return id
  }

  /** Update a document; return updated document ID. */
  def update(id: Int, doc: Array[Byte]): Int = {
    if (doc.length > Collection.MAX_DOC_ROOM)
      throw new IllegalArgumentException(s"Document is too large, it exceeds ${Collection.MAX_DOC_ROOM}")
    if (id > appendAt)
      throw new IllegalArgumentException(s"Document $id does not exist")

    val len = doc.length
    buf.position(id)
    val valid = buf.getInt()
    if (valid == Collection.DOC_INVALID)
      return id

    // Not a document header?
    if (valid != Collection.DOC_VALID)
      throw new IllegalArgumentException(s"Document $id does not exist")

    val room = buf.getInt()

    // Not a document / document header corruption?
    if (room > Collection.MAX_DOC_ROOM) {
      Collection.LOG.severe(s"Document $id has a header corruption - repair collection?")
      return id
    }

    if (room >= len) {
      buf.put(doc)
      buf.put(" ".*(room - len).getBytes())
      return id
    } else {
      delete(id)
      return insert(doc)
    }
  }

  /** Delete a document. */
  def delete(id: Int) {
    if (id > appendAt)
      throw new IllegalArgumentException(s"Document $id does not exist")

    buf.position(id)
    val valid = buf.getInt()
    if (valid != Collection.DOC_INVALID)
      if (valid == Collection.DOC_VALID) {
        buf.position(id)
        buf.putInt(0)
      } else
        throw new IllegalArgumentException(s"Document $id does not exist")
  }

  /** Iterate through all documents. */
  def foreach(f: Array[Byte] => Unit) {
    buf.position(0)

    // While the entry is not empty
    while (buf.getLong() != 0) {
      buf.position(buf.position() - Collection.DOC_HEADER_SIZE)
      f(read(buf.position()))
    }
  }

  override def workOn(work: CollectionWork, promise: Promise[CollectionWork]) {
    work match {
      case CollectionInsert(doc, pos) =>
        try {
          pos.data = insert(doc)
          promise.success(work)
        } catch {
          case e: Exception => promise.failure(e)
        }
      case CollectionUpdate(id, doc, pos) =>
        try {
          pos.data = update(id, doc)
          promise.success(work)
        } catch {
          case e: Exception => promise.failure(e)
        }
      case CollectionDelete(id) =>
        try {
          delete(id)
          promise.success(work)
        } catch {
          case e: Exception => promise.failure(e)
        }
      case CollectionRead(id, data) =>
        try {
          data.data = read(id)
          promise.success(work)
        } catch {
          case e: Exception => promise.failure(e)
        }
      case CollectionIterate(f) =>
        try {
          foreach(f)
          promise.success(work)
        } catch {
          case e: Exception => promise.failure(e)
        }
    }
  }
}