package net.houzuo.aurinko2.storage

import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import scala.math.max
import java.util.logging.Logger

object AppendFile {
  val LOG = Logger.getLogger(classOf[AppendFile].getName())
}

abstract class AppendFile(
  protected val fc: FileChannel,
  protected val growBy: Int,
  protected val minSize: Int) {

  if (fc == null || !fc.isOpen())
    throw new IllegalArgumentException("File channel is null or closed")
  if (growBy < 1024)
    throw new IllegalArgumentException("File growth is too small (< 1KB)")

  protected var buf = fc.map(MapMode.READ_WRITE, 0, max(minSize, fc.size()))
  protected var appendAt = buf.limit / 2

  // Find next appending position using binary search (the position must have data 0L)
  private var left = 0
  private var right = buf.limit
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

  // Fix "off by one"
  if (appendAt % 2 == 1)
    appendAt -= 1

  // Fix "off by one or two"
  private val int1 = buf.getInt()
  private val int2 = buf.getInt()
  if (int2 != 0)
    appendAt += 8
  else if (int1 != 0)
    appendAt += 4

  AppendFile.LOG.info(s"File is opened, append position is at $appendAt")

  /** Re-map the file if more room is needed for appending the size of data. */
  protected def checkGrow(size: Int) {
    if (appendAt + size <= buf.limit)
      return

    AppendFile.LOG.info(s"Append position is at $appendAt. File is grown by $growBy bytes because there is not enough room for $size bytes.")
    force()
    buf = fc.map(MapMode.READ_WRITE, 0, buf.limit + growBy)
  }

  /** Make sure that mapped buffer is written through to storage device. */
  def force() { buf.force() }

  /** Close the file channel. */
  def close() { force(); fc.close() }
}