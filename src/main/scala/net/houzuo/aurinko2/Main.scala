package net.houzuo.aurinko2

import java.net.ServerSocket
import java.util.logging.Logger

import net.houzuo.aurinko2.logic.Database

object Main {
  val LOG = Logger getLogger "Main"
  val FLUSH_INTERVAL = 60000 // Data file flush interval in milliseconds

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println(args mkString ("|"))
      System.err println "sbt run PORT DB_DIR";
      System exit (1)
    }

    val db = new Database(args(1))

    // Regularly flush all data files
    new Thread {
      override def run() {
        while (true) {
          Thread sleep FLUSH_INTERVAL
          for (
            name <- db.all;
            col = db get name
          ) {
            col.collection force ()
            col.idIndex force ()
            col.hashes foreach { _._2._2.force() }
          }
        }
      }
    }

    // Accept incoming connections
    val server = new ServerSocket(args(0) toInt)
    while (true) {
      val incoming = server accept ()
      Main.LOG info s"Client connected from ${incoming.getRemoteSocketAddress toString}"
      new Thread { override def run() { new Worker(db, incoming) } }
    }

    // Flush all collections and close server socket when shutdown
    Runtime.getRuntime() addShutdownHook new Thread {
      override def run() {
        db.close()
        server.close()
      }
    }
  }
}
