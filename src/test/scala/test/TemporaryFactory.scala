package test

import java.io.File
import java.io.RandomAccessFile

import aurinko2.storage.Collection
import aurinko2.storage.Hash
import aurinko2.storage.Log

object TemporaryFactory {

  /** Return a new collection which will be deleted upon JVM termination. */
  def collection = {
    val tmp = File.createTempFile(System.nanoTime().toString, "Aurinko2")
    tmp.deleteOnExit()
    new Collection((new RandomAccessFile(tmp, "rw")).getChannel())
  }

  /** Return a new hash table which will be deleted upon JVM termination. */
  def hashTable(bits: Int, perBucket: Int) = {
    val tmp = File.createTempFile(System.nanoTime().toString, "Aurinko2")
    tmp.deleteOnExit()
    new Hash((new RandomAccessFile(tmp, "rw")).getChannel(), bits, perBucket)
  }

  /** Return a log structure which will be deleted upon JVM termination. */
  def log: Log = {
    val tmp = File.createTempFile(System.nanoTime().toString, "Aurinko2")
    tmp.deleteOnExit()
    return new Log(new RandomAccessFile(tmp, "rw").getChannel())
  }
}