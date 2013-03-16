package net.houzuo.aurinko2

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.future

import net.houzuo.aurinko2.logic.Database

object Main {
  val FLUSH_INTERVAL = 60000 // Data file flush interval in milliseconds

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("sbt run PORT DB_DIR");
      System.exit(1)
    }

    val db = new Database(args(1))

    // Regularly flush all data files
    future {
      while (true) {
        Thread.sleep(FLUSH_INTERVAL)
        for (
          name <- db.all;
          col = db.get(name)
        ) {
          col.collection.force()
          col.idIndex.force()
          col.hashes foreach { _._2._2.force() }
        }
      }
    }
  }
}
