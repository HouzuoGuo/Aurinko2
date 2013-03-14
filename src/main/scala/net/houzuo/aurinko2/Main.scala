package net.houzuo.aurinko2

import java.net.ServerSocket

import net.houzuo.aurinko2.logic.Database

object Main {

  /**
   * Number of threads used in:
   * - Indexing all documents
   * - Benchmark logic.Collection
   */
  var parallelLevel = Runtime.getRuntime().availableProcessors() * 2

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("sbt run PORT DB_DIR");
      System.exit(1)
    }
    val db = new Database(args(1))

    // Flush everything every minute
    new Thread {
      while (true) {
        Thread.sleep(60000)
        db.all foreach { name =>
          val col = db.get(name)
          col.collection.force()
          col.idIndex.force()
          col.hashes foreach { _._2._2.force() }
        }
      }
    }.start()

    // Listen to incoming connections
    var server: ServerSocket = null
    try {
      server = new ServerSocket(args(0).toInt)
      while (true)
        new Worker(db, server.accept())
    } catch {
      case e: Exception => System.err.println()
    } finally {
      if (server != null && !server.isClosed())
        server.close()
    }
  }
}
