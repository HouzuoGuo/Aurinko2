package aurinko2.logic

import java.io.File
import java.io.RandomAccessFile
import java.nio.file.Paths
import java.util.logging.Logger

import scala.collection.mutable.HashMap
import scala.concurrent.Await
import scala.concurrent.duration.DurationLong
import scala.xml.Elem
import scala.xml.XML
import scala.xml.XML.loadString

import aurinko2.io.SimpleIO.spit
import aurinko2.storage.{Collection => CollFile}
import aurinko2.storage.CollectionDelete
import aurinko2.storage.CollectionInsert
import aurinko2.storage.CollectionRead
import aurinko2.storage.CollectionUpdate
import aurinko2.storage.Hash
import aurinko2.storage.Log
import aurinko2.storage.Output

object Collection {
  val LOG = Logger.getLogger(classOf[Collection].getName())
}

class Collection(val path: String) {

  private val writeLock = new Object()
  private val configFilename = Paths.get(path, "config").toString

  Collection.LOG.info(s"Opening collection $path")

  private val testOpen = new File(path)

  if (!(testOpen.exists() && testOpen.isDirectory() &&
    testOpen.canRead() && testOpen.canWrite() && testOpen.canExecute())) {
    throw new Exception(s"Collection directory $path does not exist or is not RWX to you")
  }

  // Load configuration file
  val configFile = new File(configFilename)

  if (!configFile.exists())
    if (!configFile.createNewFile())
      throw new Exception(s"Collection does not have config file and failed to create $path/config")
    else {
      spit(configFile.getAbsolutePath(), Seq("<root></root>"))
      Collection.LOG.info(s"Empty config file $path/config has been created")
    }

  if (!(configFile.canRead() && configFile.canWrite()))
    throw new Exception(s"Config file $configFilename is not RW to you")

  var config: Elem = null
  try {
    config = XML.loadFile(configFile)
  } catch {
    case e: org.xml.sax.SAXParseException =>
      Collection.LOG.severe(s"XML parser cannot parse $path/config, manually repair the collection?")
  }

  // Load hash indexes
  val hashes = new HashMap[Seq[String], Tuple2[String, Hash]]
  if (config != null)
    (config \ "hashes").foreach { hash =>
      hash.attribute("file") match {
        case Some(filename) =>
          Collection.LOG.info(s"Loading hash index $filename")
          hashes.put(((hash \ "path").map { _.text }).toSeq,
            (filename.toString,
              new Hash(new RandomAccessFile(filename.toString, "rw").getChannel(),
                hash.attribute("bits") match {
                  case Some(bits) =>
                    bits.toString.toInt
                  case None =>
                    Collection.LOG.warning("No number of hash bits defined - using default 12")
                    12
                }, hash.attribute("per_bucket") match {
                  case Some(entries) =>
                    entries.toString.toInt
                  case None =>
                    Collection.LOG.warning("No number of hash bits defined - using default 100")
                    100
                })))
        case None =>
          Collection.LOG.severe("An index exists in configuration file but without a file name. It is skipped.")
      }
    }

  val collection = new CollFile(new RandomAccessFile(Paths.get(path, "data").toString, "rw").getChannel())
  val log = new Log(new RandomAccessFile(Paths.get(path, "log").toString, "rw").getChannel())

  Collection.LOG.info(s"Successfully loaded collection $path")

  // Index management
  def index(path: Seq[String], bits: Int, perBucket: Int) {
    if (hashes.contains(path))
      throw new Exception(s"Collection ${this.path} already has $path indexed")

    val last = path(path.size - 1)
    if (last.length() > 100)
      throw new Exception(s"Last path segment ($last) is too long (> 100 characters)")

    val filename = last + System.nanoTime().toString
    hashes += ((path, (filename, new Hash(new RandomAccessFile(filename.toString, "rw").getChannel(), bits, perBucket))))
    saveConfig()
  }

  def unindex(path: Seq[String]) {
    if (!hashes.contains(path))
      throw new Exception(s"Collection ${this.path} does not have $path indexed")

    val filename = hashes(path)._1
    if (!new File(filename).delete()) {
      Collection.LOG.severe(s"Cannot delete index file $filename")
      throw new Exception(s"Cannot delete index file $filename")
    }

    hashes -= path
    saveConfig()
  }

  // Document management

  /** Read a document given document ID, return the read document, or <code>null</code> if the document is no longer valid. */
  def read(id: Int): Elem = {
    val work = CollectionRead(id, new Output[Array[Byte]](null))
    Await.result(collection.offer(work).future, Int.MaxValue nanosecond)
    if (work.data.data == null)
      return null
    return loadString(new String(work.data.data))
  }

  /** Insert a document, return its ID or -1 if non-blocking. */
  def insert(doc: Elem, wait: Boolean = false): Int = {
    val work = CollectionInsert(doc.toString.getBytes(), new Output[Int](0))
    writeLock.synchronized {
      val p = collection.offer(work)
      if (!wait)
        return -1

      Await.result(p.future, Int.MaxValue nanosecond)
      return work.pos.data
    }
  }

  /** Update a document given its ID and new document element, return updated document's ID or -1 if non-blocking. */
  def update(id: Int, doc: Elem, wait: Boolean = false): Int = {
    val work = CollectionUpdate(id, doc.toString.getBytes(), new Output[Int](0))
    writeLock.synchronized {
      val p = collection.offer(work)
      if (!wait)
        return -1

      Await.result(p.future, Int.MaxValue nanosecond)
      return work.pos.data
    }
  }

  /** Delete a document given its ID. */
  def delete(id: Int, wait: Boolean = false) {
    val work = CollectionDelete(id)
    writeLock.synchronized {
      val p = collection.offer(work)
      if (!wait)
        return
      Await.result(p.future, Int.MaxValue nanosecond)
    }
  }

  // Configuration and other

  def saveConfig() {

    // Copy existing config to .bak
    val source = scala.io.Source.fromFile("file.txt")
    spit(Paths.get(path, "config.bak").toString, Seq(source.mkString), false)
    source.close()

    // Overwrite current config file
    spit(configFilename,
      Seq(<root>{
        hashes.map {
          hash =>
            <hash file={ hash._2._1 } bits={ hash._2._2.hashBits.toString } per_bucket={ hash._2._2.perBucket.toString }>
              {
                hash._1.map { segment =>
                  <path>
                    { segment }
                  </path>
                }
              }
            </hash>
        }
      }</root>.toString),
      false)
  }

  /** Flush all collection changes to disk. */
  def save() {
    log.force()
    collection.force()
    for (hash <- hashes)
      hash._2._2.force()
    Collection.LOG.info(s"Collection $path saved at ${System.currentTimeMillis()}")
  }

  /** Save and close the collection. */
  def close() {
    log.close()
    collection.close()
    for (hash <- hashes)
      hash._2._2.close()
    Collection.LOG.info(s"Collection $path closed at ${System.currentTimeMillis()}")
  }
}