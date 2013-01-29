package aurinko2.storage

import java.nio.channels.FileChannel
import scala.math.ceil
import scala.collection.mutable.ListBuffer

object Log {
  val GROWTH = 67108864 // Log file grows 64MB when full
  val BLOCK_HEADER_SIZE = 8 // Header: time stamp (long)
  val BLOCK_SIZE = 256 // Entry is separated into blocks of 256 bytes
}

class Log(override protected val fc: FileChannel) extends AppendFile(fc, Log.GROWTH) {

  /** Insert a log entry. */
  def insert(log: Array[Byte]): Int = {
    var id = -1
    var data = log

    // Put padding spaces into log entry to fill up an entry block
    if (log.length % Log.BLOCK_SIZE != 0)
      data = Array.concat(log, " ".*(log.length % Log.BLOCK_SIZE).getBytes())
    fc.synchronized {
      id = appendAt
      val time = System.nanoTime
      buf.position(appendAt)

      // Write entry blocks - each block consists of time stamp and data
      for (slice <- Array.range(0, data.length, Log.BLOCK_SIZE)) {
        buf.putLong(time)
        buf.put(data.slice(slice, slice + Log.BLOCK_SIZE))
      }
      appendAt += data.length + (data.length / Log.BLOCK_SIZE) * Log.BLOCK_HEADER_SIZE
    }
    return id
  }

  /** Return log entry read at the position. */
  def read(id: Int): Array[Byte] = {
    val slices = new ListBuffer[Array[Byte]]()
    var timestamp = -1L
    fc.synchronized {
      buf.position(id)
      do {

        // Remember entry's time stamp
        if (timestamp == -1)
          timestamp = buf.getLong()
        else
          buf.position(buf.position() + Log.BLOCK_HEADER_SIZE)
        val slice = new Array[Byte](Log.BLOCK_SIZE)
        buf.get(slice)
        slices += slice

        // Keep on reading, until the next entry has a different time stamp 
      } while (buf.position() < buf.limit() - Log.BLOCK_HEADER_SIZE && buf.getLong() == timestamp)
    }
    return slices.toArray.flatten
  }

  /** Iterate through all log entries. */
  def foreach()(f: Array[Byte] => Unit) {
    fc.synchronized {
      buf.position(0)
      while (buf.position() < buf.limit() - Log.BLOCK_HEADER_SIZE) {
        f(read(buf.position()))

        // Because "read" looks ahead next block's time stamp,
        // so here buffer position should move back
        buf.position(buf.position() - Log.BLOCK_HEADER_SIZE)
      }
    }
  }
}