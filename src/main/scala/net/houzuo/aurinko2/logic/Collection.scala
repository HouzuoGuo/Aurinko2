package net.houzuo.aurinko2.logic

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
import scala.math.min
import scala.xml.Elem
import scala.xml.NodeSeq
import scala.xml.NodeSeq.seqToNodeSeq
import scala.xml.XML
import scala.xml.XML.loadString

import net.houzuo.aurinko2.io.SimpleIO.spit
import net.houzuo.aurinko2.storage.{ Collection => CollFile }
import net.houzuo.aurinko2.storage.CollectionDelete
import net.houzuo.aurinko2.storage.CollectionInsert
import net.houzuo.aurinko2.storage.CollectionRead
import net.houzuo.aurinko2.storage.CollectionUpdate
import net.houzuo.aurinko2.storage.Hash
import net.houzuo.aurinko2.storage.HashGetAll
import net.houzuo.aurinko2.storage.HashPut
import net.houzuo.aurinko2.storage.HashRemove
import net.houzuo.aurinko2.storage.HashSync
import net.houzuo.aurinko2.storage.HashWork
import net.houzuo.aurinko2.storage.Output

object Collection {
  val LOG = Logger.getLogger(classOf[Collection].getName())

  /** "Get into" an XML document, given a path. */
  def getIn(nodes: NodeSeq, path: List[String]): List[String] = {
    if (path.size == 0) {
      (nodes map { node =>
        if (node.child.size > 0 && node.child(0).isInstanceOf[Elem])
          node.toString // Index XML element
        else
          node.child.mkString("") // Index XML non-element
      }).toList
    } else {
      val ret = new ListBuffer[String]
      path match {
        case first :: rest => nodes foreach { node => ret ++= getIn(node \ first, rest) }
        case Nil           =>
      }
      ret.toList
    }
  }
}

class Collection(val path: String) {

  private val write = new ReentrantLock
  private val configFilename = Paths.get(path, "config").toString

  Collection.LOG.info(s"Opening collection $path")

  // Test open collection directory
  private val testOpen = new File(path)

  if (!(testOpen.exists() && testOpen.isDirectory() &&
    testOpen.canRead() && testOpen.canWrite() && testOpen.canExecute()))
    throw new Exception(s"Collection directory $path does not exist or is not RWX to you")

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
      throw new Exception(s"XML parser cannot parse $path/config, manually repair the config file?")
  }

  // Parse collection indexes
  val hashes = new HashMap[List[String], Tuple2[String, Hash]]
  (config \ "hash").foreach { hash =>
    hash.attribute("file") match {
      case Some(filename) =>
        Collection.LOG.info(s"Loading hash index $filename")
        hashes.put(((hash \ "path").map { _.text }).toList,
          (filename.text,
            new Hash(new RandomAccessFile(filename.text, "rw").getChannel(),
              hash.attribute("bits") match {
                case Some(bits) => bits.text.toInt
                case None       => throw new Exception(s"Index $filename has hash number of bits undefined")
              },
              hash.attribute("per_bucket") match {
                case Some(entries) => entries.text.toInt
                case None          => throw new Exception(s"Index $filename has index entries per bucket undefined")
              })))
      case None =>
        Collection.LOG.severe("An index exists in configuration file but without a file name. It is skipped.")
    }
  }

  // Open data files
  val collection = new CollFile(new RandomAccessFile(Paths.get(path, "data").toString, "rw").getChannel())
  val idIndex = new Hash(new RandomAccessFile(Paths.get(path, "id").toString, "rw").getChannel(), 14, 100)

  Collection.LOG.info(s"Successfully loaded collection $path")

  /** Create a new index. */
  def index(path: List[String], bits: Int, perBucket: Int) {
    if (hashes.contains(path))
      throw new Exception(s"Collection ${this.path} already has $path indexed")

    val lastSegment = path(path.size - 1)
    val filename = Paths.get(this.path, lastSegment.substring(0, min(100, lastSegment.length())) + System.nanoTime().toString).toString
    val newIndex = new Hash(new RandomAccessFile(filename.toString, "rw").getChannel(), bits, perBucket)
    hashes += ((path, (filename, newIndex)))

    // Index all the documents
    all foreach { id =>
      read(id) match {
        case Some(doc) =>
          for (toIndex <- Collection.getIn(doc, path))
            newIndex.offer(HashPut(toIndex.hashCode(), id))
        case None =>
      }
    }
    Await.result(newIndex.offer(HashSync(() => {})).future, Int.MaxValue second)

    saveConfig()
  }

  /** Delete an index. */
  def unindex(path: List[String]) {
    if (!hashes.contains(path))
      throw new Exception(s"Collection ${this.path} does not have $path indexed")

    val filename = hashes(path)._1
    if (!new File(filename).delete())
      throw new Exception(s"Cannot delete index file $filename")

    hashes -= path
    saveConfig()
  }

  /** Read a document given document ID, return the read document, or <code>null</code> if the ID is invalid. */
  def read(id: Int): Option[Elem] = {
    val work = CollectionRead(id, new Output[Array[Byte]](null))
    Await.result(collection.offer(work).future, Int.MaxValue second)
    if (work.data.data == null)
      return None
    try {
      return Some(loadString(new String(work.data.data)))
    } catch {
      case e: Exception =>
        Collection.LOG.warning(s"Document cannot be parsed as XML: ${new String(work.data.data)}")
        return None
    }
  }

  /** Put a document into all indexes. */
  def indexDoc(doc: Elem, id: Int) = {
    val promises = new ListBuffer[Promise[HashWork]]
    hashes foreach { index =>
      for (toIndex <- Collection.getIn(doc, index._1))
        promises += index._2._2.offer(HashPut(toIndex.hashCode(), id))
    }
    promises.toList
  }

  /** Remove a document from all indexes. */
  def unindexDoc(doc: Elem, id: Int) = {
    val promises = new ListBuffer[Promise[HashWork]]
    hashes map { index =>
      for (toIndex <- Collection.getIn(doc, index._1))
        promises += index._2._2.offer(HashRemove(toIndex.hashCode(), 1, (_, value) => value == id))
    }
    promises.toList
  }

  /** Insert a document, return its ID. */
  def insert(doc: Elem) = {

    // Insert document to collection
    val colInsert = CollectionInsert(doc.toString.getBytes, new Output[Int](0))
    Await.result(collection.offer(colInsert).future, Int.MaxValue second)

    // Insert to log , ID index and other indexes
    val idPromise = idIndex.offer(HashPut(colInsert.pos.data.hashCode, colInsert.pos.data))
    val indexPromises = indexDoc(doc, colInsert.pos.data)

    // Wait for log and indexes
    idPromise :: indexPromises foreach { promise => Await.result(promise.future, Int.MaxValue second) }

    colInsert.pos.data
  }

  /** Update a document given its ID and new document element, return updated document's ID. */
  def update(id: Int, doc: Elem) = {
    read(id) match {
      case Some(oldDoc) =>

        // Update document, remove old indexed value
        val colUpdate = CollectionUpdate(id, doc.toString.getBytes, new Output[Int](0))
        val updatePromise = collection.offer(colUpdate)
        val unindexPromises = unindexDoc(oldDoc, id)
        val idRemovePromise = idIndex.offer(HashRemove(id.hashCode(), 1, (_, value) => value == id))

        // Wait for document update
        Await.result(updatePromise.future, Int.MaxValue second)
        val indexPromises = indexDoc(doc, colUpdate.pos.data)
        val idPutPromise = idIndex.offer(HashPut(colUpdate.pos.data.hashCode, colUpdate.pos.data))

        // Wait for indexes
        idPutPromise :: idRemovePromise :: indexPromises ::: unindexPromises foreach { promise => Await.result(promise.future, Int.MaxValue second) }

        Some(colUpdate.pos.data)
      case None => None
    }
  }

  /** Delete a document given its ID. */
  def delete(id: Int) {
    read(id) match {
      case Some(oldDoc) =>

        // Delete document
        val colPromise = collection.offer(CollectionDelete(id))

        // Unindex old document
        val unindexPromises = unindexDoc(oldDoc, id)

        // Unindex ID
        val idPromise = idIndex.offer(HashRemove(id.hashCode(), 1, (_, value) => value == id))

        // Wait for collection and indexes
        Await.result(colPromise.future, Int.MaxValue second)
        idPromise :: unindexPromises foreach { promise => Await.result(promise.future, Int.MaxValue second) }
      case None =>
    }
  }

  /** Get all document IDs. */
  def all = {
    val work = HashGetAll(new Output[List[Tuple2[Int, Int]]](null))
    Await.result(idIndex.offer(work).future, Int.MaxValue second)
    work.result.data.map(_._2)
  }

  /** Save collection configurations, currently there is only indexes configuration. */
  def saveConfig() {

    // Backup existing config to .bak
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
                  <path>{ segment }</path>
                }
              }
            </hash>
        }
      }</root>.toString),
      false)
  }

  /** Flush all collection changes to disk. */
  def save() {
    collection.force()
    hashes foreach { _._2._2.force() }
    Collection.LOG.info(s"Collection $path saved at ${System.currentTimeMillis()}")
  }

  /** Save and close the collection. */
  def close() {
    collection.close()
    hashes foreach { _._2._2.force() }
    Collection.LOG.info(s"Collection $path closed at ${System.currentTimeMillis()}")
  }
}
