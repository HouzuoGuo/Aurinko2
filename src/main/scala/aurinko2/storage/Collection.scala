package aurinko2.storage

import java.nio.MappedByteBuffer
import scala.xml.Elem
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode

/**
 * Collection is a series of documents. Collection file grows incrementally by
 * 64 MB each time.
 * New documents are appended to the empty space in collection file.
 *
 * Each document is laid out as following:
 *  Validity   Room     Document          Padding
 * |--------|--------|-----------|----------------------|
 *  4 bytes  4 bytes   len bytes   2 * len bytes of ' '
 */
class Collection(override val fc: FileChannel) extends AppendableMemoryMappedFile(fc, 67108864) {
  /**
   * Return document at the specified position including its padding spaces.
   */
  def get(pos: Int): String = {
    var data: Array[Byte] = null
    fc.synchronized {
      buf.position(pos + 4)
      data = new Array[Byte](buf.getInt())
      buf.get(data)
    }
    return new String(data)
  }

  /**
   * Append serialised document, leave 2x its size as room for growing.
   */
  def insert(doc: String) {
    val len = doc.length()
    val room = len * 3
    val data = doc.getBytes()
    val padding = " ".*(len * 2).getBytes()
    fc.synchronized {
      checkRemap(room)
      buf.position(_appendAt)
      buf.putInt(1)
      buf.putInt(room)
      buf.put(data)
      buf.put(padding)
      _appendAt += 8 + room
    }
  }

  /**
   * Update a stored document.
   * If there is no room to grow, it will be deleted and re-inserted.
   * Return true only if document was re-inserted.
   */
  def update(doc: String, pos: Int) {
    val data = doc.getBytes()
    fc.synchronized {
      buf.position(pos)
      if (buf.getInt() == 0)
        return
      val room = buf.getInt()
      val len = doc.length()
      if (room >= len) {
        buf.put(data)
        buf.put(" ".*(room - len).getBytes())
      } else {
        delete(pos)
        insert(doc)
      }
    }
  }

  /**
   * Mark document as deleted.
   */
  def delete(pos: Int) {
    fc.synchronized {
      buf.position(pos)
      buf.putInt(0)
    }
  }
}
