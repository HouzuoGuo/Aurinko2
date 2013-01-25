package aurinko2

import java.io.RandomAccessFile
import java.io.File
import aurinko2.storage.Collection

object Main {

  def main(args: Array[String]): Unit = {
    val raf = new RandomAccessFile("/tmp/aurinko2", "rw")
    val col = new Collection(raf.getChannel())
    col.insert("abc")
    col.insert("def")
    col.insert("ghi")

  }

}
