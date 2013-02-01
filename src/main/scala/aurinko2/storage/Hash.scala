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
class Hash(override protected val fc: FileChannel) extends AppendFile(fc, Hash.GROWTH) {
  // Fix append position
  if (appendAt % Hash.BUCKET_SIZE != 0)
    appendAt += Hash.BUCKET_SIZE - appendAt % Hash.BUCKET_SIZE

  /** Return total number of buckets. */
  private def numberOfBuckets = appendAt / Hash.BUCKET_SIZE

  /** Return next chained bucket number. */
  private def next(bucket: Int): Int = {
    at(bucket)
    return buf.getInt()
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
    buf.position(bucket * Hash.BUCKET_SIZE)
  }

  /** Put a new bucket in the bucket chain. */
  private def grow(bucket: Int) {
    checkGrow(Hash.BUCKET_SIZE)
    at(last(bucket))
    buf.putInt(numberOfBuckets)
    appendAt += Hash.BUCKET_SIZE
  }

  private def scan(key: Int, limit: Int, procFun: Int => Unit, filter: (Int, Int) => Boolean): List[Int] = {
    val keyHash = key.hashCode
    val startBucket = Hash.hashKey(keyHash)
    val lastBucket = last(startBucket)
    val result = new ListBuffer[Int]()

    // From first entry of first bucket to the last entry of last bucket
    for (bucket <- startBucket to lastBucket; entry <- 0 until Hash.PER_BUCKET) {
      buf.position(bucket * Hash.BUCKET_SIZE + entry * Hash.ENTRY_SIZE)
      val entryPos = buf.position()
      val validity = buf.getInt()
      val key = buf.getInt()
      val value = buf.getInt()

      // If reached result limit, or entry is empty (last entry)
      if (result.size == limit || (validity == 0 && key == 0 && value == 0))
        return result.toList

      if (validity != Hash.ENTRY_VALID && validity != Hash.ENTRY_INVALID)
        Hash.LOG.severe(s"Hash index file is corrupted - there is no valid entry header at $entryPos")
      else if (key == keyHash && filter(key, value)) {
        procFun(entryPos)
        result += key
      }
    }
    return result.toList
  }
}