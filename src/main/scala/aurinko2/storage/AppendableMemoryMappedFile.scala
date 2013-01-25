package aurinko2.storage

import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode

abstract class AppendableMemoryMappedFile(protected val fc: FileChannel, protected val growBy: Int) {
  protected var buf = fc.map(MapMode.READ_WRITE, 0, Math.max(growBy, fc.size()))
  protected var appendAt = buf.limit() / 2;

  // Find next append position
  {
    var left = 0
    var right = buf.limit() - 1
    while (right - left > 1) {
      println("left " + left + " now " + appendAt + " right " + right)
      buf.position(appendAt)
      if (buf.getInt() == 0) {
        appendAt -= (appendAt - left) / 2
        right = appendAt
      } else {
        appendAt += (right - appendAt) / 2
        left = appendAt
      }
    }
  }

  /**
   * Re-map buffer if there is no enough room to append content.
   * Return true only if re-mapping happened.
   */

  protected def checkRemap(room: Int): Boolean = {
    if (appendAt + room > buf.limit()) {
      buf.force()
      buf = fc.map(MapMode.READ_WRITE, 0, fc.size() + Math.max(growBy, room))
      return true
    }
    return false
  }

  /**
   * Flush buffer.
   */
  def force = buf.force

  /**
   * Close file.
   */
  def close() {
    buf.force()
    fc.force(true)
    fc.close()
  }
}
