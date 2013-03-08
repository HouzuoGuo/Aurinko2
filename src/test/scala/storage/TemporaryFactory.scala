package aurinko2.test.storage

import java.io.File
import java.io.RandomAccessFile

import aurinko2.storage.Collection
import aurinko2.storage.Hash

object TemporaryFactory {

  /** Return a new collection which will be deleted upon JVM termination. */
  def collection = {
    val tmp = File.createTempFile("Aurinko2", System.nanoTime().toString)
    tmp.deleteOnExit()
    new Collection((new RandomAccessFile(tmp, "rw")).getChannel())
  }

  /** Return a new hash table which will be deleted upon JVM termination. */
  def hashTable(bits: Int, perBucket: Int) = {
    val tmp = File.createTempFile("Aurinko2", System.nanoTime().toString)
    tmp.deleteOnExit()
    new Hash((new RandomAccessFile(tmp, "rw")).getChannel(), bits, perBucket)
  }

}