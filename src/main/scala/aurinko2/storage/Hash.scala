package aurinko2.storage

import java.nio.channels.FileChannel
import java.util.logging.Logger

import scala.collection.mutable.ListBuffer
import scala.concurrent.Promise
import scala.math.max
import scala.math.pow

// Workloads
abstract class HashWork
case class HashGet(key: Int, limit: Int, filter: (Int, Int) => Boolean,
  result: Output[List[Tuple2[Int, Int]]]) extends HashWork
case class HashPut(key: Int, value: Int) extends HashWork
case class HashRemove(key: Int, limit: Int, filter: (Int, Int) => Boolean) extends HashWork
case class HashBarrier extends HashWork

object Hash {
  val LOG = Logger.getLogger(classOf[Hash].getName())

  /*
   * Hash file grows by 64MB when full.
   * Hash table is made of buckets, each consists of:
   * - bucket header (int - next bucket in chain)
   * - hash entries (int validity, int key and int value)
   */
  val GROWTH = 67108864
  val BUCKET_HEADER_SIZE = 4
  val ENTRY_SIZE = 12
  val ENTRY_VALID = 1
  val ENTRY_INVALID = 0
}

class Hash(
  override protected val fc: FileChannel,
  val hashBits: Int,
  val perBucket: Int)
  extends AppendFile(
    fc,
    Hash.GROWTH, pow(2, hashBits).toInt * (Hash.BUCKET_HEADER_SIZE + Hash.ENTRY_SIZE * perBucket))
  with WorkSerialized[HashWork] {

  // Size of a bucket full of entries, including bucket header
  val bucketSize = Hash.BUCKET_HEADER_SIZE + Hash.ENTRY_SIZE * perBucket

  // Fix append position
  appendAt = max(appendAt, pow(2, hashBits).toInt * (Hash.BUCKET_HEADER_SIZE + Hash.ENTRY_SIZE * perBucket))
  if (appendAt % bucketSize != 0)
    appendAt += bucketSize - appendAt % bucketSize

  Hash.LOG.info(s"Hash table file is opened. Bucket size is $bucketSize, initial file size is ${minSize}")

  /** Return the last N bits of the integer key. It is used for choosing a bucket when inserting into hash table. */
  def hashKey(key: Int) = key & ((1 << hashBits) - 1)

  /** Return total number of buckets. */
  private def numberOfBuckets = appendAt / bucketSize

  /** Return next chained bucket number. */
  private def next(bucket: Int): Int = {
    buf.position(bucket * bucketSize)
    val nextBucket = buf.getInt()
    if (nextBucket < bucket && nextBucket != 0) {
      Hash.LOG.severe(s"bucket chain $bucket is a loop - repair collection?")
      return 0
    }
    return nextBucket
  }

  /** Return last chained bucket number. */
  private def last(bucket: Int): Int = {
    var curr = bucket
    while (next(curr) != 0)
      curr = next(curr)
    return curr
  }

  /** Put a new bucket in the bucket chain. */
  private def grow(bucket: Int) {
    Hash.LOG.info(s"Growing a bucket in chain $bucket, new bucket's number is ${numberOfBuckets}")
    buf.position(last(bucket) * bucketSize)
    buf.putInt(numberOfBuckets)
    checkGrow(bucketSize)
    appendAt += bucketSize
  }

  /** Process no more than a limited number of entries in the key's bucket chain. Return processed entries. */
  private def scan(key: Int, limit: Int, procFun: Int => Unit, filter: (Int, Int) => Boolean): List[Tuple2[Int, Int]] = {
    val startBucket = hashKey(key)
    val result = new ListBuffer[Tuple2[Int, Int]]()
    var bucket = startBucket
    do {
      for (entry <- 0 until perBucket) {
        buf.position(bucket * bucketSize + Hash.BUCKET_HEADER_SIZE + entry * Hash.ENTRY_SIZE)
        val entryPos = buf.position()
        val validity = buf.getInt()
        val entryKey = buf.getInt()
        val value = buf.getInt()

        // If reached result limit, or entry is empty (last entry)
        if (result.size == limit || (validity == 0 && entryKey == 0 && value == 0))
          return result.toList

        if (validity != Hash.ENTRY_VALID && validity != Hash.ENTRY_INVALID)
          Hash.LOG.severe(s"Hash corruption - invalid entry header $entryPos")
        else if (validity == Hash.ENTRY_VALID && entryKey == key && filter(entryKey, value)) {
          procFun(entryPos)
          result += Tuple2(entryKey, value)
        }
      }
      bucket = next(bucket)
    } while (bucket != 0)
    return result.toList
  }

  /** Insert the key-value pair into hash table. */
  def put(key: Int, value: Int) {
    val startBucket = hashKey(key)
    var bucket = startBucket
    do {
      for (entry <- 0 until perBucket) {
        buf.position(bucket * bucketSize + Hash.BUCKET_HEADER_SIZE + entry * Hash.ENTRY_SIZE)
        if (buf.getInt() == Hash.ENTRY_INVALID) {
          buf.position(buf.position() - 4)
          buf.putInt(Hash.ENTRY_VALID)
          buf.putInt(key)
          buf.putInt(value)
          return
        }
      }
      bucket = next(bucket)
    } while (bucket != 0)

    // All buckets are full - make a new bucket and re-put
    grow(startBucket)
    put(key, value)
  }

  /** Return value(s) to which the key is mapped. */
  def get(key: Int, limit: Int, filter: (Int, Int) => Boolean) =
    scan(key, limit, (_: Int) => {}, filter)

  /** Remove key-value pairs. */
  def remove(key: Int, limit: Int, filter: (Int, Int) => Boolean) {
    scan(key, limit,
      (pos: Int) => { buf.position(pos); buf.putInt(Hash.ENTRY_INVALID) },
      filter)
  }

  override def workOn(work: HashWork, promise: Promise[HashWork]) {
    work match {
      case HashGet(key, limit, filter, result) =>
        try {
          result.data = get(key, limit, filter)
          promise.success(work)
        } catch {
          case e: Exception => promise.failure(e)
        }
      case HashPut(key, value) =>
        try {
          put(key, value)
          promise.success(work)
        } catch {
          case e: Exception => promise.failure(e)
        }
      case HashRemove(key, limit, filter) =>
        try {
          remove(key, limit, filter)
          promise.success(work)
        } catch {
          case e: Exception => promise.failure(e)
        }
      case HashBarrier() =>
        promise.success(work)
    }
  }
}