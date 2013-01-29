package aurinko2.storage

import java.nio.channels.FileChannel

object Hash {
  val GROWTH = 67108864 // Hash file grows by 64MB when full
  val HASH_BITS = 12 // Last 12 bits of key are taken as hash 
  val PER_BUCKET = 100 // There are 100 hash entries per bucket
  val BUCKET_HEADER_SIZE = 4 // Bucket header: next chained bucket number (int)
  val ENTRY_SIZE = 12 // Entry: validity (int), key (int), value (int)
  val BUCKET_SIZE = BUCKET_HEADER_SIZE + ENTRY_SIZE * PER_BUCKET
  val ENTRY_VALID = 1
}
class Hash(override protected val fc: FileChannel) extends AppendFile(fc, Hash.GROWTH) {
  private def atBucket(bucket: Int) {
  }
  private def grow(bucket: Int) {
  }
}