package aurinko2.storage

import java.nio.channels.FileChannel
import java.util.logging.Logger

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
    Collection.GROWTH) {

  /** Return document read at the position; return null if the document is no longer valid. */
  def read(id: Int): Array[Byte] = {
    if (id > appendAt)
      throw new IllegalArgumentException(s"Document ID $id is out of range")

    buf.position(id)
    val valid = buf.getInt()
    if (valid == Collection.DOC_INVALID)
      return null

    // Not a document header?
    if (valid != Collection.DOC_VALID)
      throw new IllegalArgumentException(s"There is no document at $id")

    val room = buf.getInt()

    // Possible document header corruption, better repair the collection
    if (room > Collection.MAX_DOC_ROOM) {
      Collection.LOG.severe(s"Document corruption $id")
      return null
    }
    val data = Array.ofDim[Byte](room)
    buf.get(data)
    return data
  }

  /** Insert a document; return inserted document ID. */
  def insert(doc: Array[Byte]): Int = {
    if (doc.length > Collection.MAX_DOC_SIZE)
      throw new IllegalArgumentException("Document " + new String(doc) + " exceeds MAX_DOC_SIZE")

    var id = -1
    val len = doc.length
    val room = len + len * Collection.DOC_PADDING
    val padding = " ".*(len * Collection.DOC_PADDING).getBytes()
    id = appendAt
    checkGrow(room)
    buf.position(appendAt)
    buf.putInt(Collection.DOC_VALID)
    buf.putInt(room)
    buf.put(doc)
    buf.put(padding)
    appendAt += Collection.DOC_HEADER_SIZE + room
    return id
  }

  /** Update a document; return updated document ID. */
  def update(id: Int, doc: Array[Byte]): Int = {
    if (doc.length > Collection.MAX_DOC_ROOM)
      throw new IllegalArgumentException("Document " + new String(doc) + " exceeds MAX_DOC_ROOM")
    if (id > appendAt)
      throw new IllegalArgumentException(s"There is no document at $id")

    val len = doc.length
    buf.position(id)
    val valid = buf.getInt()
    if (valid == Collection.DOC_INVALID)
      return id

    // Not a document header?
    if (valid != Collection.DOC_VALID)
      throw new IllegalArgumentException(s"There is no document at $id")

    val room = buf.getInt()

    // Not a document / document header corruption?
    if (room > Collection.MAX_DOC_ROOM) {
      Collection.LOG.severe(s"Document corruption $id")
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
      throw new IllegalArgumentException(s"Document ID $id is out of range")

    buf.position(id)
    val valid = buf.getInt()
    if (valid != Collection.DOC_INVALID)
      if (valid == Collection.DOC_VALID) {
        buf.position(id)
        buf.putInt(0)
      } else
        throw new IllegalArgumentException(s"No document to delete at $id")
  }
}