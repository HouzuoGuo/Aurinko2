package net.houzuo.aurinko2

import java.net.ServerSocket
import java.util.logging.Logger

import net.houzuo.aurinko2.logic.Database

object Main {
  val LOG = Logger getLogger "Main"
  val FLUSH_INTERVAL = 60000 // Data file flush interval in milliseconds

  def main(args: Array[String]): Unit = {
    if (args.length != 2 && args.length != 3) {
      println(args mkString ("|"))
      System.err println "sbt run PORT DB_DIR [benchmark]";
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
          ) col save
        }
      }
    } start

    // Flush all collections on shutdown
    Runtime.getRuntime addShutdownHook new Thread {
      override def run() { db close }
    }

    val server = new ServerSocket(args(0) toInt)

    // Accept incoming connections
    new Thread {
      override def run() {
        while (true) {
          val incoming = server.accept
          Main.LOG info s"Client connected from ${incoming.getRemoteSocketAddress toString}"
          new Thread { override def run() { new Worker(db, incoming) } } start
        }
      }
    } start

    // Run benchmark if asked
    if (args.length == 3 && "benchmark".equals(args(2)))
      Benchmark(args(0) toInt)
  }
}
