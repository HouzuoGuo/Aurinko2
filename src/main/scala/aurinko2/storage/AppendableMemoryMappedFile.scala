package aurinko2.storage

import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode

abstract class AppendableMemoryMappedFile(val fc: FileChannel, val growBy: Int) {
  protected var buf = fc.map(MapMode.READ_WRITE, 0, fc.size())
  protected var _appendAt = 0;

  // Find next append position
  {
    var left = 0
    var right = buf.limit()
    while (left != right) {
      _appendAt = (right - left) / 2
      if (buf.get(_appendAt) == 0)
        right = _appendAt
      else
        left = _appendAt
    }
  }

  /**
   * Re-map buffer if there is no enough room to append content.
   * Return true only if re-mapping happened.
   */

  protected def checkRemap(room: Int): Boolean = {
    if (_appendAt + room > buf.limit()) {
      buf.force()
      buf = fc.map(MapMode.READ_WRITE, 0, fc.size() + growBy)
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
