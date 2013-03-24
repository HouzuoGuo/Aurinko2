package net.houzuo.aurinko2.logic

import java.io.File
import java.io.IOException
import java.io.RandomAccessFile
import java.nio.file.Paths
import java.text.SimpleDateFormat
import java.util.Date
import java.util.logging.Logger

import scala.Array.canBuildFrom
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.Promise
import scala.concurrent.duration.DurationInt
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
import net.houzuo.aurinko2.storage.Output

object Collection {
  val IO_TIMEOUT = 120000 // IO waiting timeout in milliseconds
  private val LOG = Logger getLogger classOf[Collection].getName

  /** "Get into" an XML document, given a path. */
  def getIn(nodes: NodeSeq, path: List[String]): List[String] = {
    if (path.size == 0) {
      nodes map { _.toString } toList
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
  private val configFilename = Paths.get(path, "config").toString

  Collection.LOG info s"Opening collection $path"

  // Test open collection directory
  private val testOpen = new File(path)

  if (!(testOpen.exists() && testOpen.isDirectory() &&
    testOpen.canRead() && testOpen.canWrite() && testOpen.canExecute()))
    throw new IOException(s"$path directory does not exist or you do not have its RWX permissions")

  // Load configuration file
  val configFile = new File(configFilename)

  if (!configFile.exists())
    if (configFile.createNewFile()) {
      spit(configFile.getAbsolutePath(), Seq("<root></root>"))
      Collection.LOG info s"Empty config file $path/config has been created"
    } else
      throw new IOException(s"Collection does not have config file and failed to create it: $path/config")

  if (!(configFile.canRead() && configFile.canWrite()))
    throw new IOException(s"Config file $configFilename is not RW to you")

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
    hash attribute "file" match {
      case Some(filename) =>
        Collection.LOG info s"Loading hash index $filename"
        hashes.put((hash \ "path") map { _.text } toList,
          (filename text,
            new Hash(new RandomAccessFile(Paths.get(path, filename text) toString, "rw") getChannel,
              hash attribute "bits" match {
                case Some(bits) => bits.text toInt
                case None       => throw new Exception(s"Index $filename has hash number of bits undefined")
              },
              hash attribute "per_bucket" match {
                case Some(entries) => entries.text toInt
                case None          => throw new Exception(s"Index $filename has index entries per bucket undefined")
              })))
      case None =>
        Collection.LOG severe "An index exists in configuration file but without a file name. It is skipped."
    }
  }

  // Open data files
  val collection = new CollFile(new RandomAccessFile(Paths.get(path, "data") toString, "rw") getChannel)
  val idIndex = new Hash(new RandomAccessFile(Paths.get(path, "id") toString, "rw") getChannel, 14, 100)

  Collection.LOG info s"Successfully loaded collection $path"

  private def sync[T](p: Promise[T]) = Await.result(p future, Collection.IO_TIMEOUT millisecond)
  private def sync[T](p: Iterable[Promise[T]]) = p foreach { each => Await.result(each future, Collection.IO_TIMEOUT millisecond) }

  /** Create a new index. */
  def index(path: List[String], bits: Int, perBucket: Int) {
    if (hashes.contains(path))
      throw new Exception(s"Collection ${this.path} already has $path indexed")

    val lastSegment = path(path.size - 1)
    val filename = lastSegment.substring(0, min(100, lastSegment length)) + System.nanoTime() toString
    val newIndex = new Hash(new RandomAccessFile(Paths.get(this.path, filename) toString, "rw") getChannel, bits, perBucket)
    hashes += ((path, (filename, newIndex)))

    // Index documents given their ID
    def indexDocs(ids: Seq[Int]) {
      ids foreach { id =>
        read(id) match {
          case Some(doc) =>
            for (toIndex <- Collection.getIn(doc, path))
              newIndex.offer(HashPut(toIndex hashCode, id))
          case None =>
        }
      }
    }

    val ids = all().toArray
    if (ids.size > 0) {
      val perThread = ids.size / Runtime.getRuntime().availableProcessors * 2

      // When there are not enough documents, index them all using single thread
      if (perThread < 10)
        indexDocs(ids)
      else {

        // Using multiple threads to index all documents
        val indexers = for (i <- Array.range(0, ids.size, perThread)) yield new Thread {
          override def run() {
            indexDocs(ids.slice(i, i + perThread))
          }
        }
        indexers foreach { _.start() }
        indexers foreach { _.join() }
      }
      sync(newIndex offer HashSync(() => {}))
    }

    saveConfig()
  }

  /** Delete an index. */
  def unindex(path: List[String]) {
    if (!hashes.contains(path))
      throw new Exception(s"Collection ${this.path} does not have $path indexed")

    val filename = hashes(path)._1
    if (!new File(Paths.get(this.path, filename) toString).delete())
      throw new Exception(s"Cannot delete index file $filename")

    hashes -= path
    saveConfig()
  }

  /** Read a document given document ID, return the read document, or <code>null</code> if the ID is invalid. */
  def read(id: Int): Option[Elem] = {
    val work = CollectionRead(id, new Output[Array[Byte]](null))
    sync(collection offer work)
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
  def indexDoc(doc: Elem, id: Int) =
    for (
      index <- hashes;
      toIndex <- Collection.getIn(doc, index._1)
    ) yield index._2._2 offer HashPut(toIndex hashCode, id)

  /** Remove a document from all indexes. */
  def unindexDoc(doc: Elem, id: Int) =
    for (
      index <- hashes;
      toIndex <- Collection.getIn(doc, index._1)
    ) yield index._2._2 offer HashRemove(toIndex hashCode, 1, (_, value) => value == id)

  /** Insert a document, return its ID. */
  def insert(doc: Elem) = {

    // Insert document to collection
    val colInsert = CollectionInsert(doc.toString.getBytes, new Output[Int](0))
    sync(collection offer colInsert)

    // Insert to indexes
    val idPromise = idIndex offer HashPut(colInsert.pos.data.hashCode, colInsert.pos.data)
    val indexPromises = indexDoc(doc, colInsert.pos.data)

    // Wait for them to finish
    sync(idPromise); sync(indexPromises)

    colInsert.pos.data
  }

  /** Update a document given its ID and new document element, return updated document's ID. */
  def update(id: Int, doc: Elem) = {
    read(id) match {
      case Some(oldDoc) =>

        // Update document, remove old indexed value
        val colUpdate = CollectionUpdate(id, doc.toString.getBytes, new Output[Int](0))
        val updatePromise = collection offer colUpdate
        val unindexPromises = unindexDoc(oldDoc, id)
        val idRemovePromise = idIndex offer HashRemove(id.hashCode(), 1, (_, value) => value == id)

        // Wait for document update
        sync(updatePromise)
        val indexPromises = indexDoc(doc, colUpdate.pos.data)
        val idPutPromise = idIndex offer HashPut(colUpdate.pos.data.hashCode, colUpdate.pos.data)

        // Wait for indexes
        sync(idPutPromise); sync(idRemovePromise); sync(indexPromises)

        Some(colUpdate.pos.data)
      case None => None
    }
  }

  /** Delete a document given its ID. */
  def delete(id: Int) {
    read(id) match {
      case Some(oldDoc) =>

        // Delete document
        val colPromise = collection offer CollectionDelete(id)

        // Unindex old document
        val unindexPromises = unindexDoc(oldDoc, id)

        // Unindex ID
        val idPromise = idIndex offer HashRemove(id.hashCode(), 1, (_, value) => value == id)

        // Wait for collection and indexes
        sync(colPromise); sync(idPromise); sync(unindexPromises)
      case None =>
    }
  }

  /** Get all document IDs. */
  def all() = {
    val work = HashGetAll(new Output[List[Tuple2[Int, Int]]](null))
    sync(idIndex offer work)
    work.result.data.map(_._2)
  }

  /** Save collection configurations, currently there is only indexes configuration. */
  def saveConfig() {

    // Backup existing config to .bak
    val source = scala.io.Source.fromFile(Paths.get(path, "config") toString)
    spit(Paths.get(path, "config.bak") toString, Seq(source.mkString), false)
    source.close()

    // Overwrite current config file
    spit(configFilename,
      Seq(<root>{
        hashes.map {
          hash =>
            <hash file={ hash._2._1 } bits={ hash._2._2.hashBits toString } per_bucket={ hash._2._2.perBucket toString }>
              { hash._1.map { segment => <path>{ segment }</path> } }
            </hash>
        }
      }</root>.toString),
      false)
  }

  /** Return number of queued operations. */
  def load = (hashes.map { hash => hash._1 -> hash._2._2.queueLength }).toMap ++ Map("idIndex" -> idIndex.queueLength, "data" -> collection.queueLength)

  /** Flush all data files. */
  def save() {
    collection.force()
    hashes foreach { _._2._2.force() }
    Collection.LOG.info(s"Collection $path saved at ${new SimpleDateFormat("yyyy-MM-DD HH:mm:ss").format(new Date)}")
  }

  /** Save all data files and then close them. Do not use this collection object after calling close. */
  def close() {
    collection.close()
    hashes foreach { _._2._2.force() }
    Collection.LOG.info(s"Collection $path closed at ${new SimpleDateFormat("yyyy-MM-DD HH:mm:ss").format(new Date)}")
  }
}
