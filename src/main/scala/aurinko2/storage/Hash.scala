package aurinko2.storage

import java.nio.channels.FileChannel
import scala.collection.mutable.ListBuffer
import java.util.logging.Logger

object Hash {
  val GROWTH = 67108864 // Hash file grows by 64MB when full
  val HASH_BITS = 12 // Last 12 bits of key are taken as hash 
  val PER_BUCKET = 100 // There are 100 hash entries per bucket
  val BUCKET_HEADER_SIZE = 4 // Bucket header: next chained bucket number (int)
  val ENTRY_SIZE = 12 // Entry: validity (int), key (int), value (int)
  val BUCKET_SIZE = BUCKET_HEADER_SIZE + ENTRY_SIZE * PER_BUCKET
  val ENTRY_VALID = 1
  val ENTRY_INVALID = 0
  val LOG = Logger.getLogger(classOf[Hash].getName())
  def hashKey(key: Int): Int = {
    return key & (Hash.HASH_BITS << 1)
  }
}
class Hash(override protected val fc: FileChannel) extends AppendFile(fc, Hash.GROWTH, Hash.HASH_BITS ^ 2 * Hash.BUCKET_SIZE) {
  // Fix append position
  if (appendAt % Hash.BUCKET_SIZE != 0)
    appendAt += Hash.BUCKET_SIZE - appendAt % Hash.BUCKET_SIZE

  /** Return total number of buckets. */
  private def numberOfBuckets = appendAt / Hash.BUCKET_SIZE

  /** Return next chained bucket number. */
  private def next(bucket: Int): Int = {
    at(bucket)
    val nextBucket = buf.getInt()
    if (nextBucket <= bucket) {
      Hash.LOG.severe(s"Hash index file is corrupted - there is a loop in bucket chain $bucket")
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
    if (bucket < 0) {
      Hash.LOG.severe(s"Bucket number may not be negative - $bucket")
    }
    buf.position(bucket * Hash.BUCKET_SIZE)
  }

  /** Put a new bucket in the bucket chain. */
  private def grow(bucket: Int) {
    fc.synchronized {
      force()
      checkGrow(Hash.BUCKET_SIZE)
      at(last(bucket))
      buf.putInt(numberOfBuckets)
      appendAt += Hash.BUCKET_SIZE
    }
  }

  /** Process no more than limited number of entries in the key's bucket chain. Return processed entries. */
  private def scan(hashedKey: Int, limit: Int, procFun: Int => Unit, filter: (Int, Int) => Boolean): List[Int] = {
    val startBucket = Hash.hashKey(hashedKey)
    val result = new ListBuffer[Int]()
    var bucket = startBucket

    while (bucket != 0) {
      for (entry <- 0 until Hash.PER_BUCKET) {
        buf.position(bucket * Hash.BUCKET_SIZE + entry * Hash.ENTRY_SIZE)
        val entryPos = buf.position()
        val validity = buf.getInt()
        val entryKey = buf.getInt()
        val value = buf.getInt()

        // If reached result limit, or entry is empty (last entry)
        if (result.size == limit || (validity == 0 && entryKey == 0 && value == 0))
          return result.toList

        if (validity != Hash.ENTRY_VALID && validity != Hash.ENTRY_INVALID)
          Hash.LOG.severe(s"Hash index file is corrupted - there is no valid entry header at $entryPos")
        else if (entryKey == hashedKey && filter(entryKey, value)) {
          procFun(entryPos)
          result += entryKey
        }
      }
      bucket = next(bucket)
    }
    return result.toList
  }

  /** Insert the key-value pair into hash table. */
  def put(hashedKey: Int, value: Int) {
    val startBucket = Hash.hashKey(hashedKey)
    var bucket = startBucket
    fc.synchronized {
      while (bucket != 0) {
        for (entry <- 0 until Hash.PER_BUCKET) {
          buf.position(bucket * Hash.BUCKET_SIZE + entry * Hash.ENTRY_SIZE)
          if (buf.getInt() == Hash.ENTRY_INVALID) {
            buf.putInt(hashedKey)
            buf.putInt(value)
            return
          }
        }
        bucket = next(bucket)
      }
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