package net.houzuo.aurinko2.test.logic

import java.io.File
import java.nio.file.Files

import scala.collection.mutable.ListBuffer

import net.houzuo.aurinko2.io.SimpleIO.rmrf
import net.houzuo.aurinko2.logic.Collection

object TemporaryFactory {
  val tempDirs = new ListBuffer[File]

  // Delete all temporary directories.
  Runtime.getRuntime().addShutdownHook(new Thread {
    override def run() {
      for (dir <- tempDirs)
        rmrf(dir)
    }
  });

  def collection = {
    val dir = Files.createTempDirectory(null)
    tempDirs += new File(dir.toString())
    new Collection(dir.toString())
  }
}