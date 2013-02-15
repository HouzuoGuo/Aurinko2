package aurinko2.storage

import java.nio.channels.FileChannel
import java.util.logging.Logger
import scala.collection.mutable.ListBuffer
import scala.math.{ max, pow }
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.LinkedBlockingQueue

object Hash {

  // Hash file grows by 64MB when full
  val GROWTH = 67108864

  // Bucket header: next chained bucket number (int)
  val BUCKET_HEADER_SIZE = 4

  // Entry: validity (int), key (int), value (int)
  val ENTRY_SIZE = 12
  val ENTRY_VALID = 1
  val ENTRY_INVALID = 0

  val LOG = Logger.getLogger(classOf[Hash].getName())
}

class Hash(
  override protected val fc: FileChannel,
  val hashBits: Int,
  val perBucket: Int)

  extends AppendFile(
    fc,
    Hash.GROWTH, pow(2, hashBits).toInt * (Hash.BUCKET_HEADER_SIZE + Hash.ENTRY_SIZE * perBucket)) {

  // Queued file system access
  val queue = new LinkedBlockingQueue[Function0[Unit]]

  // Size of a bucket full of entries, including bucket header
  val bucketSize = Hash.BUCKET_HEADER_SIZE + Hash.ENTRY_SIZE * perBucket

  // Fix append position
  appendAt = max(appendAt, pow(2, hashBits).toInt * (Hash.BUCKET_HEADER_SIZE + Hash.ENTRY_SIZE * perBucket))
  if (appendAt % bucketSize != 0)
    appendAt += bucketSize - appendAt % bucketSize

  /** Return the last N bits of the integer key. It is used for choosing a bucket when inserting into hash table. */
  private def hashKey(key: Int) = key & ((1 << hashBits) - 1)

  /** Return total number of buckets. */
  private def numberOfBuckets = appendAt / bucketSize

  /** Return next chained bucket number. */
  private def next(bucket: Int): Int = {
    at(bucket)
    val nextBucket = buf.getInt()
    if (nextBucket < bucket && nextBucket != 0) {
      Hash.LOG.severe(s"Hash corruption - loop in chain $bucket")
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

  /** Position buffer to beginning of the bucket. */
  private def at(bucket: Int) {
    if (bucket < 0)
      throw new IllegalArgumentException(s"Negative bucket number $bucket")

    buf.position(bucket * bucketSize)
  }

  /** Put a new bucket in the bucket chain. */
  private def grow(bucket: Int) {
    fc.synchronized {
      at(last(bucket))
      buf.putInt(numberOfBuckets)
      checkGrow(bucketSize)
      appendAt += bucketSize
    }
  }

  /** Process no more than a limited number of entries in the key's bucket chain. Return processed entries. */
  private def scan(key: Int, limit: Int,
    procFun: Int => Unit,
    filter: (Int, Int) => Boolean,
    callback: Function1[List[Tuple2[Int, Int]], Unit]) {
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
        if (result.size == limit || (validity == 0 && entryKey == 0 && value == 0)) {
          callback(result.toList)
          return
        }

        if (validity != Hash.ENTRY_VALID && validity != Hash.ENTRY_INVALID)
          Hash.LOG.severe(s"Hash corruption - invalid entry header $entryPos")
        else if (validity == Hash.ENTRY_VALID && entryKey == key && filter(entryKey, value)) {
          procFun(entryPos)
          result += Tuple2(entryKey, value)
        }
      }
      bucket = next(bucket)
    } while (bucket != 0)
    callback(result.toList)
  }

  /** Insert the key-value pair into hash table. */
  def put(key: Int, value: Int) {
    val startBucket = hashKey(key)
    var bucket = startBucket
    queue.offer(() => {
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
    })
  }

  /** Return value(s) to which the key is mapped. */
  def get(key: Int, limit: Int, filter: (Int, Int) => Boolean) = {
    scan(key, limit, (_: Int) => {}, filter, (result: List[Tuple2[Int, Int]]) => {
    })
  }

  /** Remove key-value pairs. */
  def remove(key: Int, limit: Int, filter: (Int, Int) => Boolean) = {
    queue.offer(() => {
      scan(key, limit,
        (pos: Int) => { buf.position(pos); buf.putInt(Hash.ENTRY_INVALID) },
        filter, (_: Any) => {})
    })
  }

  // File system access synchroniser
  new Thread {
    override def run() {
      while (true)
        queue.poll()()
    }
  }.start();
}