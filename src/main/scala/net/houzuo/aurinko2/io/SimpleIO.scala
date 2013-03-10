package net.houzuo.aurinko2.io

import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import java.io.IOException

object SimpleIO {

  /** Overwrite or append the lines to the destination file. Create the file if it does not exist. */
  def spit(filename: String, lines: Seq[String], append: Boolean = false) {
    val file = new File(filename)
    if (!file.exists())
      if (!file.createNewFile())
        throw new IOException(s"Cannot create file $filename")

    if (!file.canWrite())
      throw new IOException(s"You do not have permission to write to $filename")

    val writer = new BufferedWriter(new FileWriter(file, append))
    writer.write(lines.mkString("\n"))
    writer.close()
  }

  /** Remove a directory and everything underneath it.*/
  def rmrf(dir: File) {
    if (dir.isFile())
      dir.delete()
    else {
      for (file <- dir.listFiles())
        rmrf(file)
      dir.delete()
    }
  }
}