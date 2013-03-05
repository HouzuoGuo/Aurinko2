package aurinko2.logic

import java.io.File
import java.io.RandomAccessFile
import java.nio.file.Paths
import java.util.concurrent.locks.ReentrantLock
import java.util.logging.Logger
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.Promise
import scala.concurrent.duration.DurationLong
import scala.xml.Elem
import scala.xml.NodeSeq
import scala.xml.XML
import scala.xml.XML.loadString
import aurinko2.io.SimpleIO.spit
import aurinko2.storage.{ Collection => CollFile }
import aurinko2.storage.CollectionDelete
import aurinko2.storage.CollectionInsert
import aurinko2.storage.CollectionRead
import aurinko2.storage.CollectionUpdate
import aurinko2.storage.Hash
import aurinko2.storage.HashPut
import aurinko2.storage.HashWork
import aurinko2.storage.Log
import aurinko2.storage.LogInsert
import aurinko2.storage.Output
import aurinko2.storage.HashRemove

object Collection {
  val LOG = Logger.getLogger(classOf[Collection].getName())
}

class Collection(val path: String) {

  private val write = new ReentrantLock()
  private val configFilename = Paths.get(path, "config").toString

  Collection.LOG.info(s"Opening collection $path")

  // Test open collection directory
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

  // Parse configuration file
  var config: Elem = null
  try {
    config = XML.loadFile(configFile)
  } catch {
    case e: org.xml.sax.SAXParseException =>
      throw new Exception(s"XML parser cannot parse $path/config, manually repair the collection?")
  }

  // Parse collection indexes
  val hashes = new HashMap[List[String], Tuple2[String, Hash]]
  if (config != null)
    (config \ "hashes").foreach { hash =>
      hash.attribute("file") match {
        case Some(filename) =>
          Collection.LOG.info(s"Loading hash index $filename")
          hashes.put(((hash \ "path").map { _.text }).toList,
            (filename.text,
              new Hash(new RandomAccessFile(filename.text, "rw").getChannel(),
                hash.attribute("bits") match {
                  case Some(bits) =>
                    bits.text.toInt
                  case None =>
                    Collection.LOG.warning("No number of hash bits defined - using default 12")
                    12
                }, hash.attribute("per_bucket") match {
                  case Some(entries) =>
                    entries.text.toInt
                  case None =>
                    Collection.LOG.warning("No number of hash bits defined - using default 100")
                    100
                })))
        case None =>
          Collection.LOG.severe("An index exists in configuration file but without a file name. It is skipped.")
      }
    }

  // Open data files
  val collection = new CollFile(new RandomAccessFile(Paths.get(path, "data").toString, "rw").getChannel())
  val log = new Log(new RandomAccessFile(Paths.get(path, "log").toString, "rw").getChannel())

  Collection.LOG.info(s"Successfully loaded collection $path")

  // Index management
  def index(path: List[String], bits: Int, perBucket: Int) {
    if (hashes.contains(path))
      throw new Exception(s"Collection ${this.path} already has $path indexed")

    val filename = path(path.size - 1).substring(100) + System.nanoTime().toString
    hashes += ((path, (filename, new Hash(new RandomAccessFile(filename.toString, "rw").getChannel(), bits, perBucket))))
    saveConfig()
  }

  def unindex(path: List[String]) {
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

  def getIn(nodes: NodeSeq, path: List[String]): List[String] = {
    if (path.size == 0) {
      return (nodes map { node =>
        if (node.child.size > 0 && node.child(0).isInstanceOf[Elem])
          node.toString // Index XML element
        else
          node.child.mkString("") // Index XML non-element
      }).toList
    } else {
      val ret = new ListBuffer[String]
      path match {
        case first :: rest =>
          nodes foreach { node => ret ++= getIn(node \ first, rest) }
        case Nil =>
      }
      return ret.toList
    }
  }

  /** Read a document given document ID, return the read document, or <code>null</code> if the ID is invalid. */
  def read(id: Int): Elem = {
    val work = CollectionRead(id, new Output[Array[Byte]](null))
    Await.result(collection.offer(work).future, Int.MaxValue nanosecond)
    if (work.data.data == null)
      return null
    return loadString(new String(work.data.data))
  }

  def indexDoc(doc: Elem, id: Int): List[Promise[HashWork]] = {
    val promises = new ListBuffer[Promise[HashWork]]
    hashes foreach { index =>
      for (toIndex <- getIn(doc, index._1))
        promises += index._2._2.offer(HashPut(toIndex.hashCode(), id))
    }
    return promises.toList
  }

  def unindexDoc(doc: Elem, id: Int): List[Promise[HashWork]] = {
    val promises = new ListBuffer[Promise[HashWork]]
    hashes map { index =>
      for (toIndex <- getIn(doc, index._1))
        promises += index._2._2.offer(HashRemove(toIndex.hashCode(), 1, (_, value) => value == id))
    }
    return promises.toList
  }

  /** Insert a document, return its ID. */
  def insert(doc: Elem): Int = {

    // Insert to collection
    val colInsert = CollectionInsert(doc.toString.getBytes, new Output[Int](0))
    val colPromise = collection.offer(colInsert)
    Await.result(colPromise.future, Int.MaxValue nanosecond)

    // Insert to log and indexes
    val logPromise = log.offer(LogInsert(<i>{ doc }</i>.toString.getBytes, new Output[Int](0)))
    val indexPromises = indexDoc(doc, colInsert.pos.data)

    // Wait for log and indexes
    (logPromise +: indexPromises) foreach { promise =>
      Await.result(promise.future, Int.MaxValue nanosecond)
    }

    return colInsert.pos.data
  }

  /** Update a document given its ID and new document element, return updated document's ID. */
  def update(id: Int, doc: Elem): Int = {
    val oldDoc = read(id)
    return 0
  }

  /** Delete a document given its ID. */
  def delete(id: Int) {
    return 0
  }

  // Configuration and other

  def saveConfig() {

    // Copy existing config to .bak
    val source = scala.io.Source.fromFile(Paths.get(path, "config").toString)
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
    hashes foreach { _._2._2.force() }
    Collection.LOG.info(s"Collection $path saved at ${System.currentTimeMillis()}")
  }

  /** Save and close the collection. */
  def close() {
    log.close()
    collection.close()
    hashes foreach { _._2._2.force() }
    Collection.LOG.info(s"Collection $path closed at ${System.currentTimeMillis()}")
  }
}