package aurinko2.storage

import java.nio.channels.FileChannel

import scala.collection.mutable.ListBuffer

object Log {

  /*
   * Log file grows by 64MB when full.
   * Log file is made of entries blocks. Blocks of the same timestamp make up a log entry.
   * Each block consists of:
   * - block header (long timestamp)
   * - block content
   */

  val GROWTH = 67108864
  val BLOCK_HEADER_SIZE = 8
  val BLOCK_SIZE = 256
}

class Log(override protected val fc: FileChannel) extends AppendFile(fc, Log.GROWTH, Log.GROWTH) {

  // Fix append position
  if (appendAt % Log.BLOCK_SIZE != 0)
    appendAt += Log.BLOCK_SIZE - appendAt % Log.BLOCK_SIZE

  /** Insert a log entry. */
  def insert(log: Array[Byte]): Int = {
    var id = -1
    var data = log

    // Put padding spaces into log entry to fill up an entry block
    if (data.length % Log.BLOCK_SIZE != 0)
      data = Array.concat(log, " ".*(Log.BLOCK_SIZE - data.length % Log.BLOCK_SIZE).getBytes())

    val entrySize = data.length + (data.length / Log.BLOCK_SIZE) * Log.BLOCK_HEADER_SIZE
    checkGrow(entrySize)
    id = appendAt
    val time = System.nanoTime
    buf.position(appendAt)

    // Write entry blocks - each block consists of time stamp and data
    for (slice <- Array.range(0, data.length, Log.BLOCK_SIZE)) {
      buf.putLong(time)
      buf.put(data.slice(slice, slice + Log.BLOCK_SIZE))
    }
    appendAt += entrySize
    return id
  }

  /** Return log entry read at the position. */
  def read(id: Int): Array[Byte] = {
    if (id > appendAt)
      throw new IllegalArgumentException("Log entry ID " + id + " is out of range")

    val slices = new ListBuffer[Array[Byte]]()
    var timestamp = -1L
    buf.position(id)
    do {

      // Remember entry's time stamp
      if (timestamp == -1)
        timestamp = buf.getLong()
      val slice = new Array[Byte](Log.BLOCK_SIZE)
      buf.get(slice)
      slices += slice

      // Keep on reading, until the next entry has a different time stamp
      // Intentionally using & instead of &&
    } while (buf.position() < appendAt - Log.BLOCK_HEADER_SIZE & buf.getLong() == timestamp)
    return slices.toArray.flatten
  }

  /** Iterate through all log entries. */
  def foreach(f: Array[Byte] => Unit) {
    buf.position(0)

    // While the entry is not empty
    while (buf.getLong() != 0) {
      buf.position(buf.position() - Log.BLOCK_HEADER_SIZE)
      f(read(buf.position()))
      buf.position(buf.position() - Log.BLOCK_HEADER_SIZE)
    }
  }
}