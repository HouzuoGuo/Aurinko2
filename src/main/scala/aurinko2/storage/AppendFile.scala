package aurinko2.storage

import scala.math.max
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode

abstract class AppendFile(protected val fc: FileChannel, protected val growBy: Int, protected val minSize: Int) {
  if (fc == null || !fc.isOpen())
    throw new IllegalArgumentException("File channel is null")
  if (growBy < 1024)
    throw new IllegalArgumentException("File growth is too small (< 1KB)")

  protected var buf = fc.map(MapMode.READ_WRITE, 0, max(minSize, fc.size()))
  protected var appendAt = buf.limit / 2

  /* Find next appending position (appendAt) using binary search.
  The position must have data 0L and be as close as possible to BOF. */
  {
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
    buf.position(appendAt)
    val int1 = buf.getInt()
    val int2 = buf.getInt()
    if (int2 != 0)
      appendAt += 8
    else if (int1 != 0)
      appendAt += 4
  }

  /** Re-map the file and return true if more room is needed for appending more data. */
  protected def checkGrow(room: Int): Boolean = {
    if (room < 1)
      throw new IllegalArgumentException("room must be > 0")

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