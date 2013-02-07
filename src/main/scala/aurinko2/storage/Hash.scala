package aurinko2.storage

import java.nio.channels.FileChannel
import java.util.logging.Logger
import scala.collection.mutable.ListBuffer
import scala.math.{ max, pow }

object Hash {
  val GROWTH = 67108864 // Hash file grows by 64MB when full
  val BUCKET_HEADER_SIZE = 4 // Bucket header: next chained bucket number (int)
  val ENTRY_SIZE = 12 // Entry: validity (int), key (int), value (int)
  val ENTRY_VALID = 1
  val ENTRY_INVALID = 0
  val LOG = Logger.getLogger(classOf[Hash].getName())
}
class Hash(override protected val fc: FileChannel, val hashBits: Int, val perBucket: Int)
  extends AppendFile(fc, Hash.GROWTH, pow(2, hashBits).toInt * (Hash.BUCKET_HEADER_SIZE + Hash.ENTRY_SIZE * perBucket)) {

  // Size of a bucket full of entries, including bucket header
  val bucketSize = Hash.BUCKET_HEADER_SIZE + Hash.ENTRY_SIZE * perBucket
  def hashKey(key: Int): Int = {
    return key & ((hashBits << 1) - 1)
  }

  // Fix append position
  appendAt = max(appendAt, pow(2, hashBits).toInt * (Hash.BUCKET_HEADER_SIZE + Hash.ENTRY_SIZE * perBucket))
  if (appendAt % bucketSize != 0)
    appendAt += bucketSize - appendAt % bucketSize

  /** Return total number of buckets. */
  private def numberOfBuckets = appendAt / bucketSize

  /** Return next chained bucket number. */
  private def next(bucket: Int): Int = {
    at(bucket)
    val nextBucket = buf.getInt()
    if (nextBucket < bucket) {
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
      checkGrow(bucketSize)
      buf.putInt(numberOfBuckets)
      appendAt += bucketSize
    }
  }

  /** Process no more than limited number of entries in the key's bucket chain. Return processed entries. */
  private def scan(hashedKey: Int, limit: Int, procFun: Int => Unit, filter: (Int, Int) => Boolean): List[Tuple2[Int, Int]] = {
    val startBucket = hashKey(hashedKey)
    val result = new ListBuffer[Tuple2[Int, Int]]()
    var bucket = startBucket

    while (bucket != 0) {
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
        else if (entryKey == hashedKey && filter(entryKey, value)) {
          procFun(entryPos)
          result += Tuple2(entryKey, value)
        }
      }
      bucket = next(bucket)
    }
    return result.toList
  }

  /** Insert the key-value pair into hash table. */
  def put(hashedKey: Int, value: Int) {
    val startBucket = hashKey(hashedKey)
    var bucket = startBucket
    fc.synchronized {
      do {
        for (entry <- 0 until perBucket) {
          buf.position(bucket * bucketSize + Hash.BUCKET_HEADER_SIZE + entry * Hash.ENTRY_SIZE)
          if (buf.getInt() == Hash.ENTRY_INVALID) {
            buf.position(buf.position() - 4)
            buf.putInt(Hash.ENTRY_VALID)
            buf.putInt(hashedKey)
            buf.putInt(value)
            return
          }
        }
        bucket = next(bucket)
      } while (bucket != 0)
    }

    // All buckets are full - make a new bucket and re-put
    grow(startBucket)
    put(hashedKey, value)
  }

  /** Return value(s) to which the key is mapped. */
  def get(hashedKey: Int, limit: Int, filter: (Int, Int) => Boolean) = fc.synchronized { scan(hashedKey, limit, (_: Int) => {}, filter) }

  /** Remove key-value pairs. */
  def remove(hashedKey: Int, limit: Int, filter: (Int, Int) => Boolean) = {
    fc.synchronized {
      scan(hashedKey, limit,
        (pos: Int) => { buf.position(pos); buf.putInt(Hash.ENTRY_INVALID) },
        filter)
    }
  }
}