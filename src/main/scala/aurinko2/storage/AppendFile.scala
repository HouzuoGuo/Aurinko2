package aurinko2.storage

import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode

abstract class AppendFile(protected val fc: FileChannel, protected val growBy: Int) {
  protected var buf = fc.map(MapMode.READ_WRITE, 0, fc.size())
  protected var appendAt = buf.limit / 2

  /* Initialise next appending position (appendAt) using binary search.
  The position must have data 0L and be as close as possible to BOF. */
  if (buf.limit > 0) {
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
    }
    appendAt += 1
  }

  /** Re-map the file if more room is needed for appending more data. */
  def checkGrow(room: Int): Boolean = {
    if (appendAt + room > buf.limit) {
      force()
      buf = fc.map(MapMode.READ_WRITE, 0, buf.limit + growBy)
      return true
    }
    return false
  }

  /** Make sure that mapped buffer is written through to storage device. */
  def force() { buf.force() }

  /** Close the file channel. */
  def close() { force(); fc.close() }
}