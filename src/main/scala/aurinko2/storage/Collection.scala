package aurinko2.storage

import java.nio.channels.FileChannel

object Collection {
  val GROWTH = 67108864
}
class Collection(override protected val fc: FileChannel) extends AppendFile(fc, Collection.GROWTH) {
  def insert(doc: Array[Byte]): Int = {
    val id = appendAt
    val len = doc.length
    val room = len * 3
    val padding = " ".*(len * 2).getBytes()
    fc.synchronized {
      checkGrow(room)
      buf.position(appendAt)
      buf.putInt(1)
      buf.putInt(room)
      buf.put(doc)
      buf.put(padding)
      appendAt += 8 + room
    }
    return id
  }

  def update(id: Int, doc: Array[Byte]): Int = {
    val len = doc.length
    fc.synchronized {
      buf.position(appendAt)
      val valid = buf.getInt()
      if (valid == 0)
        return id
      val room = buf.getInt()
      if (room >= len) {
        buf.put(doc)
        buf.put(" ".*(room - len).getBytes())
        return id
      } else {
        delete(id)
        return insert(doc)
      }
    }
  }

  def delete(id: Int) {
    fc.synchronized {
      buf.position(id)
      buf.putInt(0)
    }
  }
}