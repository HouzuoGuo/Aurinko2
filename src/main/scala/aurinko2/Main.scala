package aurinko2

import java.io.RandomAccessFile
import aurinko2.storage.Collection

object Main {
  def main(args: Array[String]): Unit = {
    val raf = new RandomAccessFile("/tmp/data", "rw")
    val col = new Collection(raf.getChannel())
  }
}
