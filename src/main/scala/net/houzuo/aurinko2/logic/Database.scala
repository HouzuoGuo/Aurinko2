package net.houzuo.aurinko2.logic

import java.io.File
import java.io.IOException
import java.nio.file.Paths
import java.util.concurrent.locks.ReentrantLock
import java.util.logging.Logger

import scala.Array.canBuildFrom
import scala.collection.mutable.HashMap

import net.houzuo.aurinko2.io.SimpleIO

object Database {
  val LOG = Logger.getLogger(classOf[Database].getName())
}

class Database(val path: String) {
  private val syncher = new ReentrantLock
  private val collections = new HashMap[String, Collection]
  private val dbDir = new File(path)

  // Test open directory
  if (!(dbDir.exists() && dbDir.isDirectory() &&
    dbDir.canRead() && dbDir.canWrite() && dbDir.canExecute()))
    throw new IOException(s"$path directory does not exist or you do not have its RWX permissions")

  // Open each sub directory as collection
  try {
    new File(path).listFiles() foreach { subfile =>
      try {
        collections += ((subfile.getName(), new Collection(subfile.getAbsolutePath())))
      } catch {
        case e: Exception =>
          Database.LOG.severe(s"Failed to open collection ${subfile.getAbsolutePath()}: " + e.getMessage())
      }
    }
  } catch {
    case e: Exception => throw new IOException(s"Failed to list files in $path")
  }

  /** Create a new collection. */
  def create(name: String) {
    syncher.lock()
    try {
      if (collections.contains(name))
        throw new Exception(s"Collection $name already exists")

      val newDir = Paths.get(path, name).toString
      if (!new File(newDir).mkdirs())
        throw new IOException(s"Failed to create collection directory $newDir")

      collections += ((name, new Collection(newDir)))
    } finally { syncher.unlock() }
  }

  /** Return reference to the opened collection. */
  def get(name: String): Collection = {
    syncher.lock()
    try {
      collections.get(name) match {
        case Some(col) => return col
        case None      => throw new Exception(s"Collection $name does not exist")
      }
    } finally { syncher.unlock() }
  }

  /** Return all collection names. */
  def all: Set[String] = {
    syncher.lock()
    try {
      return collections.keySet.toSet
    } finally { syncher.unlock() }
  }

  /** Rename a collection. References to the old collection will no longer work. */
  def rename(from: String, to: String) {
    syncher.lock()
    try {
      if (!collections.get(to).isEmpty)
        throw new Exception(s"Collection name $to is already in-use")

      collections.get(from) match {
        case Some(old) =>
          old.save()
          val newPath = new File(Paths.get(path, to).toString)
          if (!newPath.mkdirs())
            throw new IOException(s"Failed to rename ${old.path} to ${newPath.getAbsolutePath()}")
          if (!new File(old.path).renameTo(newPath))
            throw new IOException(s"Failed to rename ${old.path} to ${newPath.getAbsolutePath()}")

          collections -= from
          collections += ((to, new Collection(newPath.getAbsolutePath())))
        case None => throw new Exception(s"Collection $from does not exist")
      }
    } finally { syncher.unlock() }
  }

  /** Drop (delete) a collection. */
  def drop(name: String) {
    syncher.lock()
    try {
      collections.get(name) match {
        case Some(old) =>
          SimpleIO.rmrf(new File(old.path))
          collections -= name
        case None => throw new Exception(s"Collection $name does not exist")
      }
    } finally { syncher.unlock() }
  }

  def repair(name: String) {
    syncher.lock()
    try {
      val toRepair = get(name)
      val tmp = "repair" + System.nanoTime().toString
      create(tmp)
      val tmpCol = get(tmp)

      // Copy documents from source to temporary collection
      def copyDocs(ids: Seq[Int]) {
        ids foreach { id =>
          toRepair.read(id) match {
            case Some(doc) => tmpCol.insert(doc)
            case None      =>
          }
        }
      }

      // Get all document IDs
      val ids = toRepair.all().toArray
      if (ids.size > 0) {
        val perThread = ids.size / Runtime.getRuntime().availableProcessors() * 2
        if (perThread < 10) {
          copyDocs(ids)
        } else {

          // Using multiple threads to copy documents across
          val inserts = for (i <- Array.range(0, ids.size, perThread)) yield new Thread {
            override def run() {
              copyDocs(ids.slice(i, i + perThread))
            }
          }
          inserts foreach { _.start() }
          inserts foreach { _.join() }
        }
      }

      // Start using the repaired collection
      drop(name)
      rename(tmp, name)
    } finally { syncher.unlock() }
  }

}