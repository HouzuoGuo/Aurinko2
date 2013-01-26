package aurinko2.storage

import java.nio.channels.FileChannel

object Log {
  val GROWTH = 67108864
  val BLOCK_SIZE = 256
}

class Log(override protected val fc: FileChannel) extends AppendFile(fc, Log.GROWTH) {
  def append(log: Array[Byte]): Int = {
  }
}