package aurinko2.storage

import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode

abstract class AppendFile(protected val fc: FileChannel, protected val growBy: Int) {
  protected var buf = fc.map(MapMode.READ_WRITE, 0, fc.size())
  protected var appendAt = buf.limit / 2

  if (buf.limit > 0) {
    // Find appending position - closest 0L
    var left = 0
    var right = buf.limit
    while (right - left > 1) {
      buf.position(appendAt)
      if (buf.getLong() == 0) {
        right = appendAt
        appendAt -= (appendAt - left) / 2
      } else {
        left = appendAt
        appendAt += (right - appendAt) / 2
      }
      println("Left " + left + " now " + appendAt + " right " + right)
    }
  }

  def checkGrow(room: Int): Boolean = {
    if (appendAt + room > buf.limit) {
      force()
      buf = fc.map(MapMode.READ_WRITE, 0, buf.limit + growBy)
      return true
    }
    return false
  }

  def force() { buf.force() }
  def close() { force(); fc.close() }
}